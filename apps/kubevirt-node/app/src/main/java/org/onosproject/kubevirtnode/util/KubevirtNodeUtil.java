/*
 * Copyright 2020-present Open Networking Foundation
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
package org.onosproject.kubevirtnode.util;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
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
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.impl.KubernetesClientImpl;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import org.apache.commons.lang.StringUtils;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.kubevirtnode.api.DefaultKubernetesExternalLbInterface;
import org.onosproject.kubevirtnode.api.DefaultKubevirtNode;
import org.onosproject.kubevirtnode.api.DefaultKubevirtPhyInterface;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbInterface;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeState;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.behaviour.BridgeConfig;
import org.onosproject.net.behaviour.BridgeName;
import org.onosproject.net.device.DeviceService;
import org.onosproject.ovsdb.controller.OvsdbClientService;
import org.onosproject.ovsdb.controller.OvsdbController;
import org.onosproject.ovsdb.controller.OvsdbNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Address;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.onlab.util.Tools.get;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnode.api.Constants.CALICO_PROJECT_DOMAIN;
import static org.onosproject.kubevirtnode.api.Constants.CLI_MARGIN_LENGTH;
import static org.onosproject.kubevirtnode.api.Constants.SONA_PROJECT_DOMAIN;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.MASTER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.OTHER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;

/**
 * An utility that used in KubeVirt node app.
 */
public final class KubevirtNodeUtil {

    private static final Logger log = LoggerFactory.getLogger(KubevirtNodeUtil.class);

    // Shared by every client this application builds. kubernetes-client 6.x
    // dispatches watch events through a per-client executor that
    // client.close() shuts down; a watcher that re-instantiates (config
    // update, reconnect) closes its previous client while the old web socket
    // still delivers buffered frames, and those then die with
    // RejectedExecutionException inside fabric8's SerialExecutor. An executor
    // handed in through withTaskExecutor() is left alone by close().
    private static final ExecutorService CLIENT_EXECUTOR = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setThreadFactory(groupedThreads("onos/kubevirt-node", "fabric8-%d", log))
                    .setDaemon(true)
                    .build());

    private static final String COLON_SLASH = "://";
    private static final String COLON = ":";

    private static final int HEX_LENGTH = 16;
    private static final String OF_PREFIX = "of:";
    private static final String ZERO = "0";
    private static final String INTERNAL_IP = "InternalIP";
    private static final String K8S_ROLE = "node-role.kubernetes.io";
    private static final String NODE_FEATURE_KERNEL_VERSION = "feature.node.kubernetes.io/kernel-version";
    private static final String NODE_FEATURE_KERNEL_VERSION_MAJOR = NODE_FEATURE_KERNEL_VERSION + ".major";
    private static final String NODE_FEATURE_KERNEL_VERSION_MINOR = NODE_FEATURE_KERNEL_VERSION + ".minor";
    private static final String CONTROL_PLANE = "control-plane";
    private static final String PHYSNET_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/physnet-config";
    private static final String TUNNEL_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/tunnel-config";
    private static final String DATA_IP_KEY = SONA_PROJECT_DOMAIN + "/data-ip";
    private static final String GATEWAY_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/gateway-config";
    private static final String GATEWAY_BRIDGE_NAME = "gatewayBridgeName";
    private static final String EXTERNAL_LB_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/externalLb-config";
    private static final String EXTERNAL_LB_BRIDGE_NAME = "externalLbBridgeName";
    private static final String EXTERNAL_LB_IP_KEY = SONA_PROJECT_DOMAIN + "/externalLb-ip";
    private static final String EXTERNAL_LB_GATEWAY_IP_KEY = SONA_PROJECT_DOMAIN + "/externalLb-gateway-ip";
    private static final String EXTERNAL_LB_GATEWAY_MAC_KEY = SONA_PROJECT_DOMAIN + "/externalLb-gateway-mac";
    private static final String NETWORK_KEY = "network";
    private static final String INTERFACE_KEY = "interface";
    private static final String PHYS_BRIDGE_ID = "physBridgeId";
    private static final String KAAS_ELB_GWS_KEY = "kaasElbGws";

    private static final String CALICO_VXLAN_TUNNEL_ADDR = CALICO_PROJECT_DOMAIN + "/IPv4VXLANTunnelAddr";

    private static final int PORT_NAME_MAX_LENGTH = 15;

    private static final String NO_SCHEDULE_EFFECT = "NoSchedule";
    private static final String KUBEVIRT_IO_KEY = "kubevirt.io/drain";
    private static final String DRAINING_VALUE = "draining";

    /**
     * Prevents object installation from external.
     */
    private KubevirtNodeUtil() {
    }

    /**
     * Generates endpoint URL by referring to scheme, ipAddress and port.
     *
     * @param apiConfig     kubernetes API config
     * @return generated endpoint URL
     */
    public static String endpoint(KubevirtApiConfig apiConfig) {
        return endpoint(apiConfig.scheme(), apiConfig.ipAddress(), apiConfig.port());
    }

    /**
     * Generates endpoint URL by referring to scheme, ipAddress and port.
     *
     * @param scheme        scheme
     * @param ipAddress     IP address
     * @param port          port number
     * @return generated endpoint URL
     */
    public static String endpoint(KubevirtApiConfig.Scheme scheme, IpAddress ipAddress, int port) {
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
     * Generates a CLI column length based on the longest string among the
     * given column header and values, with the CLI margin appended.
     *
     * @param header column header
     * @param values column values
     * @return column length
     */
    public static int genColumnLength(String header, Collection<String> values) {
        int length = header == null ? 0 : header.length();
        for (String value : values) {
            if (value != null && value.length() > length) {
                length = value.length();
            }
        }
        return length + CLI_MARGIN_LENGTH;
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
     * Obtains workable kubernetes client.
     *
     * @param config kubernetes API config
     * @return kubernetes client
     */
    public static KubernetesClient k8sClient(KubevirtApiConfig config) {
        if (config == null) {
            log.warn("Kubernetes API server config is empty.");
            return null;
        }

        String endpoint = endpoint(config);

        ConfigBuilder configBuilder = new ConfigBuilder().withMasterUrl(endpoint);

        if (config.scheme() == KubevirtApiConfig.Scheme.HTTPS) {
            // trustCerts short-circuits fabric8's trust manager to accept any
            // server certificate, so it must stay off for the configured CA
            // bundle to actually take part in server verification
            if (StringUtils.isNotEmpty(config.caCertData())) {
                configBuilder.withTrustCerts(false)
                        .withCaCertData(config.caCertData());
            } else {
                log.warn("No CA cert data configured for endpoint {}; " +
                        "skipping API server certificate validation.", endpoint);
                configBuilder.withTrustCerts(true);
            }

            configBuilder.withClientCertData(config.clientCertData())
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
        serialization.registerKubernetesResource(ConfigMap.class);
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
     * Gets the ovsdb client with supplied openstack node.
     *
     * @param node              kubernetes node
     * @param ovsdbPort         ovsdb port
     * @param ovsdbController   ovsdb controller
     * @return ovsdb client
     */
    public static OvsdbClientService getOvsdbClient(KubevirtNode node,
                                                    int ovsdbPort,
                                                    OvsdbController ovsdbController) {
        OvsdbNodeId ovsdb = new OvsdbNodeId(node.managementIp(), ovsdbPort);
        return ovsdbController.getOvsdbClient(ovsdb);
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
    public static boolean isOvsdbConnected(KubevirtNode node,
                                           int ovsdbPort,
                                           OvsdbController ovsdbController,
                                           DeviceService deviceService) {
        OvsdbClientService client = getOvsdbClient(node, ovsdbPort, ovsdbController);
        return deviceService.isAvailable(node.ovsdb()) &&
                client != null &&
                client.isConnected();
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
    public static synchronized void addOrRemoveSystemInterface(KubevirtNode k8sNode,
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
     * Re-structures the OVS port name.
     * The length of OVS port name should be not large than 15.
     *
     * @param portName  original port name
     * @return re-structured OVS port name
     */
    public static String structurePortName(String portName) {

        // The size of OVS port name should not be larger than 15
        if (portName.length() > PORT_NAME_MAX_LENGTH) {
            return StringUtils.substring(portName, 0, PORT_NAME_MAX_LENGTH);
        }

        return portName;
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
     * Returns the type of the given kubernetes node.
     *
     * @param node kubernetes node
     * @return node type
     */
    public static KubevirtNode.Type getNodeType(Node node) {
        Set<String> rolesFull = node.getMetadata().getLabels().keySet().stream()
                .filter(l -> l.contains(K8S_ROLE))
                .collect(Collectors.toSet());

        KubevirtNode.Type nodeType = WORKER;

        for (String roleStr : rolesFull) {
            String role = roleStr.split("/")[1];
            if (CONTROL_PLANE.equalsIgnoreCase(role) || MASTER.name().equalsIgnoreCase(role)) {
                nodeType = MASTER;
                break;
            }
        }

        Map<String, String> annots = node.getMetadata().getAnnotations();
        String gatewayConfig = annots.get(GATEWAY_CONFIG_KEY);
        if (gatewayConfig != null) {
            nodeType = GATEWAY;
        }

        return nodeType;
    }

    /**
     * Returns the kubevirt node from the node.
     *
     * @param node a raw node object returned from a k8s client
     * @return kubevirt node, or null if any of the node's SONA annotations
     *         cannot be parsed
     */
    public static KubevirtNode buildKubevirtNode(Node node) {
        String hostname = node.getMetadata().getName();
        IpAddress managementIp = null;
        IpAddress dataIp = null;

        for (NodeAddress nodeAddress:node.getStatus().getAddresses()) {
            if (nodeAddress.getType().equals(INTERNAL_IP)) {
                managementIp = IpAddress.valueOf(nodeAddress.getAddress());
                dataIp = IpAddress.valueOf(nodeAddress.getAddress());
            }
        }

        Set<String> rolesFull = node.getMetadata().getLabels().keySet().stream()
                .filter(l -> l.contains(K8S_ROLE))
                .collect(Collectors.toSet());

        KubevirtNode.Type nodeType = WORKER;

        for (String roleStr : rolesFull) {
            String role = roleStr.split("/")[1];
            if (CONTROL_PLANE.equalsIgnoreCase(role) || MASTER.name().equalsIgnoreCase(role)) {
                nodeType = MASTER;
                break;
            }
        }

        Map<String, String> labels = node.getMetadata().getLabels();
        Integer kernelVersionMajor = -1;
        Integer kernelVersionMinor = -1;
        if (labels.containsKey(NODE_FEATURE_KERNEL_VERSION_MAJOR) &&
                labels.containsKey(NODE_FEATURE_KERNEL_VERSION_MINOR)) {
            kernelVersionMajor = Integer.parseInt(labels.get(NODE_FEATURE_KERNEL_VERSION_MAJOR));
            kernelVersionMinor = Integer.parseInt(labels.get(NODE_FEATURE_KERNEL_VERSION_MINOR));
        }
        int[] kernelVersion = {kernelVersionMajor, kernelVersionMinor};

        // start to parse kubernetes annotation
        Map<String, String> annots = node.getMetadata().getAnnotations();
        String physnetConfig = annots.get(PHYSNET_CONFIG_KEY);
        String gatewayConfig = annots.get(GATEWAY_CONFIG_KEY);
        String tunnelConfig = annots.get(TUNNEL_CONFIG_KEY);
        String dataIpStr = annots.get(DATA_IP_KEY);     // Deprecated. Use tunnelConfig instead
        Set<KubevirtPhyInterface> phys = new HashSet<>();
        String gatewayBridgeName = null;

        String elbConfig = annots.get(EXTERNAL_LB_CONFIG_KEY);
        String elbIpStr = annots.get(EXTERNAL_LB_IP_KEY);
        String elbGwIpStr = annots.get(EXTERNAL_LB_GATEWAY_IP_KEY);
        String elbGwMacStr = annots.get(EXTERNAL_LB_GATEWAY_MAC_KEY);
        String calicoVxlanIpStr = annots.get(CALICO_VXLAN_TUNNEL_ADDR);
        String elbBridgeName = null;
        IpAddress elbIp = null;
        IpAddress elbGwIp = null;
        MacAddress elbGwMac = null;
        boolean vxlanInUse = calicoVxlanIpStr != null;

        KubernetesExternalLbInterface kubernetesExternalLbInterface = null;

        // each annotation is parsed in its own try, and every parse failure
        // ends the build with null: a syntax error in tunnel-config used to
        // skip the gateway-config parsing that followed it in the same try,
        // so the node came out with its label-derived type (MASTER/WORKER)
        // instead of GATEWAY and the watcher, reading that as "gateway
        // annotation removed", tore the live gateway down; minimal-json's
        // ParseException and the NPE on a missing key are unchecked and
        // escaped the caller entirely, dropping the event with nothing but a
        // stack trace; returning null lets the watcher refuse or ignore the
        // node object explicitly instead
        try {
            if (physnetConfig != null) {
                JsonArray configJson = JsonArray.readFrom(physnetConfig);

                for (int i = 0; i < configJson.size(); i++) {
                    JsonObject object = configJson.get(i).asObject();
                    JsonValue networkJson = object.get(NETWORK_KEY);
                    JsonValue intfJson = object.get(INTERFACE_KEY);
                    if (networkJson == null || intfJson == null) {
                        throw new IllegalArgumentException("physnet entry " + object +
                                " lacks the " + NETWORK_KEY + " or " + INTERFACE_KEY + " key");
                    }
                    String network = networkJson.asString();
                    String intf = intfJson.asString();
                    JsonValue jsonKaasElbs = object.get(KAAS_ELB_GWS_KEY);

                    String physBridgeId;
                    if (object.get(PHYS_BRIDGE_ID) != null) {
                        physBridgeId = object.get(PHYS_BRIDGE_ID).asString();
                    } else {
                        physBridgeId = genDpidFromName(network + intf + hostname);
                        log.trace("host {} physnet dpid for network {} intf {} is null so generate dpid {}",
                                hostname, network, intf, physBridgeId);
                    }

                    Set<String> kaasElbs = new HashSet<>();
                    if (jsonKaasElbs != null) {
                        jsonKaasElbs.asArray().forEach(jsonElb -> kaasElbs.add(jsonElb.asString()));
                    }

                    phys.add(DefaultKubevirtPhyInterface.builder()
                            .network(network)
                            .intf(intf)
                            .physBridge(DeviceId.deviceId(physBridgeId))
                            .kaasElbs(kaasElbs)
                            .build());
                }
            }
        } catch (RuntimeException e) {
            log.error("Failed to parse physnet-config annotation of node {}: {}",
                    hostname, physnetConfig, e);
            return null;
        }

        try {
            if (tunnelConfig != null) {
                JsonNode jsonNode = new ObjectMapper().readTree(tunnelConfig);
                dataIp = IpAddress.valueOf(jsonNode.get("ip").asText());
            } else if (dataIpStr != null) {
                dataIp = IpAddress.valueOf(dataIpStr);
            }
        } catch (JsonProcessingException | RuntimeException e) {
            log.error("Failed to parse tunnel-config or data-ip annotation of node {}: {}",
                    hostname, tunnelConfig != null ? tunnelConfig : dataIpStr, e);
            return null;
        }

        try {
            if (gatewayConfig != null) {
                JsonNode jsonNode = new ObjectMapper().readTree(gatewayConfig);

                nodeType = GATEWAY;
                gatewayBridgeName = jsonNode.get(GATEWAY_BRIDGE_NAME).asText();

                if (elbConfig != null && elbIpStr != null && elbGwIpStr != null) {
                    JsonNode elbJsonNode = new ObjectMapper().readTree(elbConfig);

                    elbBridgeName = elbJsonNode.get(EXTERNAL_LB_BRIDGE_NAME).asText();
                    elbIp = IpAddress.valueOf(elbIpStr);
                    elbGwIp = IpAddress.valueOf(elbGwIpStr);

                    if (elbGwMacStr != null) {
                        elbGwMac = MacAddress.valueOf(elbGwMacStr);
                    }

                    kubernetesExternalLbInterface = DefaultKubernetesExternalLbInterface.builder()
                            .externalLbBridgeName(elbBridgeName)
                            .externalLbIp(elbIp)
                            .externallbGwIp(elbGwIp)
                            .externalLbGwMac(elbGwMac)
                            .build();
                }
            }
        } catch (JsonProcessingException | RuntimeException e) {
            log.error("Failed to parse gateway-config or externalLb-config annotation of node {}: {}",
                    hostname, gatewayConfig, e);
            return null;
        }

        // if the node is taint with kubevirt.io key configured,
        // we mark this node as OTHER type, and do not add it into the cluster
        NodeSpec spec = node.getSpec();
        if (spec.getTaints() != null) {
            for (Taint taint : spec.getTaints()) {
                String effect = taint.getEffect();
                String key = taint.getKey();
                String value = taint.getValue();

                if (StringUtils.equals(effect, NO_SCHEDULE_EFFECT) &&
                    StringUtils.equals(key, KUBEVIRT_IO_KEY) &&
                    StringUtils.equals(value, DRAINING_VALUE)) {
                    nodeType = OTHER;
                }
            }
        }

        return DefaultKubevirtNode.builder()
                .hostname(hostname)
                .managementIp(managementIp)
                .dataIp(dataIp)
                .type(nodeType)
                .state(KubevirtNodeState.ON_BOARDED)
                .phyIntfs(phys)
                .gatewayBridgeName(gatewayBridgeName)
                .kubernetesExternalLbInterface(kubernetesExternalLbInterface)
                .kernelVersion(kernelVersion)
                .vxlanInUse(vxlanInUse)
                .build();
    }

    /**
     * Generates a unique dpid from given name.
     *
     * @param name name
     * @return device id in string
     */
    public static String genDpidFromName(String name) {
        if (name != null) {
            String hexString = Integer.toHexString(name.hashCode());
            return OF_PREFIX + Strings.padStart(hexString, 16, '0');
        }
        return null;
    }

    /**
     * Resolve a DNS with the given DNS server and hostname.
     *
     * @param hostname      hostname to be resolved
     * @return resolved IP address
     */
    public static IpAddress resolveHostname(String hostname) {
        try {
            InetAddress addr = Address.getByName(hostname);
            return IpAddress.valueOf(IpAddress.Version.INET, addr.getAddress());
        } catch (UnknownHostException e) {
            log.warn("Failed to resolve IP address of host {}", hostname);
        }
        return null;
    }

    /**
     * Waits for the given length of time.
     *
     * @param timeSecond the amount of time for wait in second unit
     */
    public static void waitFor(int timeSecond) {
        try {
            Thread.sleep(timeSecond * 1000L);
        } catch (Exception e) {
            log.error(e.toString());
        }
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
