/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.k8snode.util;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyMetadata;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.AnyGetterWriter;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerBuilder;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.MapSerializer;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.impl.KubernetesClientImpl;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import org.apache.commons.lang.StringUtils;
import org.onlab.packet.IpAddress;
import org.onosproject.k8snode.api.K8sApiConfig;
import org.onosproject.k8snode.api.K8sApiConfig.Scheme;
import org.onosproject.k8snode.api.K8sNode;
import org.onosproject.net.Device;
import org.onosproject.net.behaviour.BridgeConfig;
import org.onosproject.net.behaviour.BridgeName;
import org.onosproject.net.device.DeviceService;
import org.onosproject.ovsdb.controller.OvsdbClientService;
import org.onosproject.ovsdb.controller.OvsdbController;
import org.onosproject.ovsdb.controller.OvsdbNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Dictionary;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.onlab.util.Tools.get;
import static org.onlab.util.Tools.groupedThreads;

/**
 * An utility that used in kubernetes node app.
 */
public final class K8sNodeUtil {
    private static final Logger log = LoggerFactory.getLogger(K8sNodeUtil.class);

    // Shared by every client this application builds. kubernetes-client 6.x
    // dispatches watch events through a per-client executor that
    // client.close() shuts down; a watcher that re-instantiates (config
    // update, reconnect) closes its previous client while the old web socket
    // still delivers buffered frames, and those then die with
    // RejectedExecutionException inside fabric8's SerialExecutor. An executor
    // handed in through withTaskExecutor() is left alone by close().
    private static final ExecutorService CLIENT_EXECUTOR = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setThreadFactory(groupedThreads("onos/k8s-node", "fabric8-%d", log))
                    .setDaemon(true)
                    .build());

    private static final String COLON_SLASH = "://";
    private static final String COLON = ":";

    private static final int HEX_LENGTH = 16;
    private static final String OF_PREFIX = "of:";
    private static final String ZERO = "0";

    /**
     * Prevents object installation from external.
     */
    private K8sNodeUtil() {
    }

    /**
     * Checks whether the controller has a connection with an OVSDB that resides
     * inside the given kubernetes node.
     *
     * @param node              kubernetes node
     * @param ovsdbPort         OVSDB port
     * @param ovsdbController   OVSDB controller
     * @param deviceService     device service
     * @return true if the controller is connected to the OVSDB, false otherwise
     */
    public static boolean isOvsdbConnected(K8sNode node,
                                           int ovsdbPort,
                                           OvsdbController ovsdbController,
                                           DeviceService deviceService) {
        OvsdbClientService client = getOvsdbClient(node, ovsdbPort, ovsdbController);
        return deviceService.isAvailable(node.ovsdb()) &&
                client != null &&
                client.isConnected();
    }

    /**
     * Gets the ovsdb client with supplied openstack node.
     *
     * @param node              kubernetes node
     * @param ovsdbPort         ovsdb port
     * @param ovsdbController   ovsdb controller
     * @return ovsdb client
     */
    public static OvsdbClientService getOvsdbClient(K8sNode node,
                                                    int ovsdbPort,
                                                    OvsdbController ovsdbController) {
        OvsdbNodeId ovsdb = new OvsdbNodeId(node.managementIp(), ovsdbPort);
        return ovsdbController.getOvsdbClient(ovsdb);
    }

    /**
     * Adds or removes a network interface (aka port) into a given bridge of kubernetes node.
     *
     * @param k8sNode       kubernetes node
     * @param bridgeName    bridge name
     * @param intfName      interface name
     * @param deviceService device service
     * @param addOrRemove   add port is true, remove it otherwise
     */
    public static synchronized void addOrRemoveSystemInterface(K8sNode k8sNode,
                                                               String bridgeName,
                                                               String intfName,
                                                               DeviceService deviceService,
                                                               boolean addOrRemove) {


        Device device = deviceService.getDevice(k8sNode.ovsdb());
        if (device == null || !device.is(BridgeConfig.class)) {
            log.info("device is null or this device if not ovsdb device");
            return;
        }
        BridgeConfig bridgeConfig =  device.as(BridgeConfig.class);

        if (addOrRemove) {
            bridgeConfig.addPort(BridgeName.bridgeName(bridgeName), intfName);
        } else {
            bridgeConfig.deletePort(BridgeName.bridgeName(bridgeName), intfName);
        }
    }

    /**
     * Gets Boolean property from the propertyName
     * Return null if propertyName is not found.
     *
     * @param properties   properties to be looked up
     * @param propertyName the name of the property to look up
     * @return value when the propertyName is defined or return null
     */
    public static Boolean getBooleanProperty(Dictionary<?, ?> properties,
                                             String propertyName) {
        Boolean value;
        try {
            String s = get(properties, propertyName);
            value = Strings.isNullOrEmpty(s) ? null : Boolean.valueOf(s);
        } catch (ClassCastException e) {
            value = null;
        }
        return value;
    }

    /**
     * Prints out the JSON string in pretty format.
     *
     * @param mapper        Object mapper
     * @param jsonString    JSON string
     * @return pretty formatted JSON string
     */
    public static String prettyJson(ObjectMapper mapper, String jsonString) {
        try {
            Object jsonObject = mapper.readValue(jsonString, Object.class);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        } catch (IOException e) {
            log.debug("Json string parsing exception caused by {}", e);
        }
        return null;
    }

    /**
     * Generates endpoint URL by referring to scheme, ipAddress and port.
     *
     * @param scheme        scheme
     * @param ipAddress     IP address
     * @param port          port number
     * @return generated endpoint URL
     */
    public static String endpoint(Scheme scheme, IpAddress ipAddress, int port) {
        StringBuilder endpoint = new StringBuilder();
        String protocol = StringUtils.lowerCase(scheme.name());

        endpoint.append(protocol);
        endpoint.append(COLON_SLASH);
        endpoint.append(ipAddress.toString());
        endpoint.append(COLON);
        endpoint.append(port);

        return endpoint.toString();
    }

    /**
     * Generates endpoint URL by referring to scheme, ipAddress and port.
     *
     * @param apiConfig     kubernetes API config
     * @return generated endpoint URL
     */
    public static String endpoint(K8sApiConfig apiConfig) {
        return endpoint(apiConfig.scheme(), apiConfig.ipAddress(), apiConfig.port());
    }

    /**
     * Generates a DPID (of:0000000000000001) from an index value.
     *
     * @param index index value
     * @return generated DPID
     */
    public static String genDpid(long index) {
        if (index < 0) {
            return null;
        }

        String hexStr = Long.toHexString(index);

        StringBuilder zeroPadding = new StringBuilder();
        for (int i = 0; i < HEX_LENGTH - hexStr.length(); i++) {
            zeroPadding.append(ZERO);
        }

        return OF_PREFIX + zeroPadding.toString() + hexStr;
    }

    /**
     * Generates string format based on the given string length list.
     *
     * @param stringLengths a list of string lengths
     * @return string format (e.g., %-28s%-15s%-24s%-20s%-15s)
     */
    public static String genFormatString(List<Integer> stringLengths) {
        StringBuilder fsb = new StringBuilder();
        stringLengths.forEach(length -> {
            fsb.append("%-");
            fsb.append(length);
            fsb.append("s");
        });
        return fsb.toString();
    }

    /**
     * Obtains workable kubernetes client.
     *
     * @param config kubernetes API config
     * @return kubernetes client
     */
    public static KubernetesClient k8sClient(K8sApiConfig config) {
        if (config == null) {
            log.warn("Kubernetes API server config is empty.");
            return null;
        }

        String endpoint = endpoint(config);

        ConfigBuilder configBuilder = new ConfigBuilder().withMasterUrl(endpoint);

        if (config.scheme() == K8sApiConfig.Scheme.HTTPS) {
            configBuilder.withTrustCerts(true)
                    .withCaCertData(config.caCertData())
                    .withClientCertData(config.clientCertData())
                    .withClientKeyData(config.clientKeyData());

            if (StringUtils.isNotEmpty(config.token())) {
                configBuilder.withOauthToken(config.token());
            }
        }

        return buildClient(configBuilder.build());
    }

    /**
     * Returns the serialization the clients of this application are built
     * with, registering every kind the application watches or lists.
     *
     * @return kubernetes serialization with this application's kinds registered
     */
    public static KubernetesSerialization kubernetesSerialization() {
        ObjectMapper mapper = new ObjectMapper();
        // registered ahead of fabric8's own serializer modifier on purpose:
        // Jackson runs the most recently registered modifier first, and the
        // repair must see the property list after UnmatchedFieldTypeModule
        // has wrapped it
        mapper.registerModule(new SimpleModule().setSerializerModifier(new AnyGetterRepair()));
        KubernetesSerialization serialization = new KubernetesSerialization(mapper, true);
        // KubernetesDeserializer learns kinds only through ServiceLoader on the
        // context class loader and on kubernetes-model-core's loader. A Felix
        // bundle loader exposes just its own META-INF/services, so nothing
        // outside model-core is found and an unknown kind deserializes as
        // GenericKubernetesResource, which closes a typed watcher on its first
        // event. Register what this application consumes, core kinds included.
        serialization.registerKubernetesResource(Node.class);
        return serialization;
    }

    private static KubernetesClient buildClient(Config config) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            // KubernetesClientBuilder loads KubernetesClientImpl by name through
            // the context class loader; the wrapped client-api bundle does not
            // import that package, but the kubernetes-client bundle owns it
            Thread.currentThread().setContextClassLoader(
                    KubernetesClientImpl.class.getClassLoader());
            return new KubernetesClientBuilder()
                    .withConfig(config)
                    .withTaskExecutor(CLIENT_EXECUTOR)
                    // the okhttp HttpClient.Factory sits in another bundle's
                    // META-INF/services, out of ServiceLoader's reach in OSGi
                    .withHttpClientFactory(new OkHttpClientFactory())
                    .withKubernetesSerialization(kubernetesSerialization())
                    .build();
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    /**
     * Auto generates DPID from the given name.
     *
     * @param name name
     * @return auto generated DPID
     */
    public static String genDpidFromName(String name) {
        if (name != null) {
            String hexString = Integer.toHexString(name.hashCode());
            return OF_PREFIX + Strings.padStart(hexString, 16, '0');
        }

        return null;
    }

    /**
     * Puts Jackson's any-getter writer back after fabric8's
     * UnmatchedFieldTypeModule has wrapped it.
     *
     * Since Jackson 2.19 the any-getter behind a model's additionalProperties
     * is an ordinary entry of the property list, and BeanSerializerBase only
     * contextualises it when the entry still is an AnyGetterWriter. fabric8
     * 6.13, built against Jackson 2.17, wraps every property in a
     * BeanPropertyWriterDelegate from updateBuilder(), so the map serializer
     * behind additionalProperties never receives its key serializer and
     * serialising any resource that carries additional properties (every
     * custom resource, any core resource with a field the model does not
     * know) fails with a NullPointerException inside MapSerializer.
     */
    private static final class AnyGetterRepair extends BeanSerializerModifier {

        @Override
        public BeanSerializerBuilder updateBuilder(SerializationConfig config,
                                                   BeanDescription beanDesc,
                                                   BeanSerializerBuilder builder) {
            AnnotatedMember anyGetter = beanDesc.findAnyGetter();
            List<BeanPropertyWriter> props = builder.getProperties();
            if (anyGetter == null || props == null) {
                return builder;
            }
            for (int i = 0; i < props.size(); i++) {
                BeanPropertyWriter prop = props.get(i);
                if (prop instanceof AnyGetterWriter || prop.getMember() == null ||
                        !Objects.equals(prop.getMember().getMember(), anyGetter.getMember())) {
                    continue;
                }
                // mirrors what BeanSerializerFactory built before the wrapping
                JavaType anyType = anyGetter.getType();
                JsonSerializer<?> anySer = MapSerializer.construct((Set<String>) null, anyType,
                        config.isEnabled(MapperFeature.USE_STATIC_TYPING), null, null, null, null);
                BeanProperty.Std anyProp = new BeanProperty.Std(
                        PropertyName.construct(anyGetter.getName()), anyType.getContentType(),
                        null, anyGetter, PropertyMetadata.STD_OPTIONAL);
                props.set(i, new AnyGetterWriter(prop, anyProp, anyGetter, anySer));
            }
            return builder;
        }
    }
}
