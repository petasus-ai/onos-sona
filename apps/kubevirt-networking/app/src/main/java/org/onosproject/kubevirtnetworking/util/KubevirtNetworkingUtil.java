/*
 * Copyright 2021-present Open Networking Foundation
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
package org.onosproject.kubevirtnetworking.util;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.onlab.packet.ARP;
import org.onlab.packet.Ethernet;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.IpAddress;
import org.onlab.packet.IpPrefix;
import org.onlab.packet.MacAddress;
import org.onosproject.cfg.ConfigProperty;
import org.onosproject.kubevirtnetworking.api.DefaultKubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.DefaultKubevirtPort;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.KubevirtHostRoute;
import org.onosproject.kubevirtnetworking.api.KubevirtIpPool;
import org.onosproject.kubevirtnetworking.api.KubevirtLoadBalancer;
import org.onosproject.kubevirtnetworking.api.KubevirtLoadBalancerService;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkService;
import org.onosproject.kubevirtnetworking.api.KubevirtPort;
import org.onosproject.kubevirtnetworking.api.KubevirtRouter;
import org.onosproject.kubevirtnetworking.api.KubevirtRouterService;
import org.onosproject.kubevirtnode.api.DefaultKubevirtNode;
import org.onosproject.kubevirtnode.api.DefaultKubevirtPhyInterface;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbInterface;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
import org.onosproject.kubevirtnode.api.KubevirtNodeState;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.group.DefaultGroupKey;
import org.onosproject.net.group.GroupKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Address;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.onosproject.kubevirtnetworking.api.Constants.TUNNEL_TO_TENANT_PREFIX;
import static org.onosproject.kubevirtnetworking.api.KubevirtNetwork.Type.FLAT;
import static org.onosproject.kubevirtnode.api.Constants.CALICO_PROJECT_DOMAIN;
import static org.onosproject.kubevirtnode.api.Constants.SONA_PROJECT_DOMAIN;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.MASTER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.OTHER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.onosproject.net.AnnotationKeys.PORT_MAC;
import static org.onosproject.net.AnnotationKeys.PORT_NAME;

/**
 * An utility that used in KubeVirt networking app.
 */
public final class KubevirtNetworkingUtil {

    private static final Logger log = LoggerFactory.getLogger(KubevirtNetworkingUtil.class);

    private static final int PORT_NAME_MAX_LENGTH = 15;
    private static final String COLON_SLASH = "://";
    private static final String COLON = ":";
    private static final String OF_PREFIX = "of:";

    private static final String NETWORK_STATUS_KEY = "k8s.v1.cni.cncf.io/network-status";
    private static final String NAME = "name";
    private static final String NETWORK_PREFIX = "default/";
    private static final String MAC = "mac";
    private static final String IPS = "ips";
    private static final String BR_INT = "br-int";
    private static final String METADATA = "metadata";
    private static final String STATUS = "status";
    private static final String INTERFACES = "interfaces";
    private static final String NODE_NAME = "nodeName";

    private static final String NETWORK_ID = "networkId";
    private static final String NETWORK_CONFIG = "network-config";
    private static final String TYPE = "type";
    private static final String MTU = "mtu";
    private static final String SEGMENT_ID = "segmentId";
    private static final String PHYSNET_NAME = "physnetName";
    private static final String GATEWAY_IP = "gatewayIp";
    private static final String DEFAULT_ROUTE = "defaultRoute";
    private static final String CIDR = "cidr";
    private static final String HOST_ROUTES = "hostRoutes";
    private static final String DESTINATION = "destination";
    private static final String NEXTHOP = "nexthop";
    private static final String IP_POOL = "ipPool";
    private static final String START = "start";
    private static final String END = "end";
    private static final String DNSES = "dnses";

    private static final String INTERNAL_IP = "InternalIP";
    private static final String K8S_ROLE = "node-role.kubernetes.io";
    private static final String NODE_FEATURE_KERNEL_VERSION = "feature.node.kubernetes.io/kernel-version";
    private static final String NODE_FEATURE_KERNEL_VERSION_MAJOR = NODE_FEATURE_KERNEL_VERSION + ".major";
    private static final String NODE_FEATURE_KERNEL_VERSION_MINOR = NODE_FEATURE_KERNEL_VERSION + ".minor";
    private static final String PHYSNET_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/physnet-config";
    private static final String TUNNEL_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/tunnel-config";
    private static final String DATA_IP_KEY = SONA_PROJECT_DOMAIN + "/data-ip";
    private static final String GATEWAY_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/gateway-config";
    private static final String GATEWAY_BRIDGE_NAME = "gatewayBridgeName";
    private static final String NETWORK_KEY = "network";
    private static final String INTERFACE_KEY = "interface";
    private static final String PHYS_BRIDGE_ID = "physBridgeId";
    private static final String KAAS_ELB_GWS_KEY = "kaasElbGws";
    private static final String CALICO_VXLAN_TUNNEL_ADDR = CALICO_PROJECT_DOMAIN + "/IPv4VXLANTunnelAddr";
    private static final String NO_SCHEDULE_EFFECT = "NoSchedule";
    private static final String KUBEVIRT_IO_KEY = "kubevirt.io/drain";
    private static final String DRAINING_VALUE = "draining";

    /**
     * Prevents object installation from external.
     */
    private KubevirtNetworkingUtil() {
    }

    /**
     * Obtains the boolean property value with specified property key name.
     *
     * @param properties a collection of properties
     * @param name       key name
     * @return mapping value
     */
    public static boolean getPropertyValueAsBoolean(Set<ConfigProperty> properties,
                                                    String name) {
        Optional<ConfigProperty> property =
                properties.stream().filter(p -> p.name().equals(name)).findFirst();

        return property.map(ConfigProperty::asBoolean).orElse(false);
    }

    /**
     * Re-structures the OVS port name.
     * The length of OVS port name should be not large than 15.
     *
     * @param portName original port name
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
     * Prints out the JSON string in pretty format.
     *
     * @param mapper     Object mapper
     * @param jsonString JSON string
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
     * Obtains valid IP addresses of the given subnet.
     *
     * @param cidr CIDR
     * @return set of IP addresses
     */
    public static Set<IpAddress> getSubnetIps(String cidr) {
        SubnetUtils utils = new SubnetUtils(cidr);
        utils.setInclusiveHostCount(false);
        SubnetUtils.SubnetInfo info = utils.getInfo();
        Set<String> allAddresses =
                new HashSet<>(Arrays.asList(info.getAllAddresses()));

        if (allAddresses.size() > 2) {
            allAddresses.remove(info.getLowAddress());
            allAddresses.remove(info.getHighAddress());
        }

        return allAddresses.stream()
                .map(IpAddress::valueOf).collect(Collectors.toSet());
    }

    /**
     * Calculate the broadcast address from given IP address and subnet prefix length.
     *
     * @param ipAddr       IP address
     * @param prefixLength subnet prefix length
     * @return broadcast address
     */
    public static String getBroadcastAddr(String ipAddr, int prefixLength) {
        String subnet = ipAddr + "/" + prefixLength;
        SubnetUtils utils = new SubnetUtils(subnet);
        return utils.getInfo().getBroadcastAddress();
    }

    /**
     * Generates endpoint URL by referring to scheme, ipAddress and port.
     *
     * @param scheme    scheme
     * @param ipAddress IP address
     * @param port      port number
     * @return generated endpoint URL
     */
    public static String endpoint(KubevirtApiConfig.Scheme scheme, IpAddress ipAddress, int port) {
        StringBuilder endpoint = new StringBuilder();
        String protocol = org.apache.commons.lang3.StringUtils.lowerCase(scheme.name());

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
     * @param apiConfig kubernetes API config
     * @return generated endpoint URL
     */
    public static String endpoint(KubevirtApiConfig apiConfig) {
        return endpoint(apiConfig.scheme(), apiConfig.ipAddress(), apiConfig.port());
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
            configBuilder.withTrustCerts(true)
                    .withCaCertData(config.caCertData())
                    .withClientCertData(config.clientCertData())
                    .withClientKeyData(config.clientKeyData());
        }

        return new DefaultKubernetesClient(configBuilder.build());
    }

    /**
     * Obtains workable kubernetes client.
     *
     * @param service kubernetes API service
     * @return kubernetes client
     */
    public static KubernetesClient k8sClient(KubevirtApiConfigService service) {
        KubevirtApiConfig config = service.apiConfig();
        if (config == null) {
            log.error("Failed to find valid kubernetes API configuration.");
            return null;
        }

        KubernetesClient client = k8sClient(config);

        if (client == null) {
            log.error("Failed to connect to kubernetes API server.");
            return null;
        }

        return client;
    }

    /**
     * Obtains the hex string of the given segment ID with fixed padding.
     *
     * @param segIdStr segment identifier string
     * @return hex string with padding
     */
    public static String segmentIdHex(String segIdStr) {
        int segId = Integer.parseInt(segIdStr);
        return String.format("%06x", segId).toLowerCase();
    }

    /**
     * Obtains the tunnel port number with the given network and node.
     *
     * @param network kubevirt network
     * @param node    kubevirt node
     * @return tunnel port number
     */
    public static PortNumber tunnelPort(KubevirtNetwork network, KubevirtNode node) {
        switch (network.type()) {
            case VXLAN:
                return node.vxlanPort();
            case GRE:
                return node.grePort();
            case GENEVE:
                return node.genevePort();
            case STT:
                return node.sttPort();
            default:
                break;
        }
        return null;
    }

    /**
     * Obtains the kubevirt port from kubevirt VMI.
     *
     * @param nodeService kubevirt node service
     * @param networks set of existing kubevirt networks
     * @param resource  VMI definition
     * @return kubevirt ports attached to the VMI
     */
    public static Set<KubevirtPort> getPorts(KubevirtNodeService nodeService,
                                             Set<KubevirtNetwork> networks,
                                             String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            JsonNode statusJson = json.get(STATUS);
            ArrayNode interfacesJson = (ArrayNode) statusJson.get(INTERFACES);
            String vmName = parseResourceName(resource);

            KubevirtPort.Builder builder = DefaultKubevirtPort.builder();
            String nodeName = parseVmiNodeName(resource);
            if (nodeName != null && nodeService.node(nodeName) != null) {
                builder.deviceId(nodeService.node(nodeName).intgBridge());
            }

            if (interfacesJson == null) {
                return ImmutableSet.of();
            }

            Set<KubevirtPort> ports = new HashSet<>();
            for (JsonNode interfaceJson : interfacesJson) {
                JsonNode jsonName = interfaceJson.get(NAME);

                // in some cases, name attribute may not be available from the
                // interface, we skip inspect this interface
                if (jsonName == null) {
                    continue;
                }

                String name = jsonName.asText();
                KubevirtNetwork network = networks.stream()
                        .filter(n -> (NETWORK_PREFIX + n.name()).equals(name) ||
                                     (n.name() + "-net").equals(name))
                        .findAny().orElse(null);
                if (network != null && interfaceJson.get(MAC) != null) {
                    // FIXME: we do not update IP address, as learning IP address
                    // requires much more time due to the lag from VM agent
                    String mac = interfaceJson.get(MAC).asText();
                    builder.vmName(vmName)
                            .macAddress(MacAddress.valueOf(mac))
                            .networkId(network.networkId());
                    ports.add(builder.build());
                }
            }
            return ports;
        } catch (IOException e) {
            log.error("Failed to parse port info from VMI object", e);
        }
        return ImmutableSet.of();
    }

    public static String parseVmiNodeName(String resource) {
        String nodeName = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            JsonNode statusJson = json.get(STATUS);
            JsonNode nodeNameJson = statusJson.get(NODE_NAME);
            nodeName = nodeNameJson != null ? nodeNameJson.asText() : null;
        } catch (IOException e) {
            log.error("Failed to parse kubevirt VMI nodename");
        }

        return nodeName;
    }

    /**
     * Gets the number of tenant networks which have the identical segmentation ID of the given network.
     *
     * @param networkService    network service
     * @param network           kubevirt network
     * @return number of tenant networks
     */
    public static long numOfDupSegNetworks(KubevirtNetworkService networkService, KubevirtNetwork network) {
        return networkService.networks().stream()
                .filter(n -> Objects.equals(network.segmentId(), n.segmentId()))
                .filter(n -> !Objects.equals(network.networkId(), n.networkId())).count();
    }

    /**
     * Obtains the tunnel bridge to tenant bridge patch port number.
     *
     * @param deviceService device service
     * @param node    kubevirt node
     * @param network kubevirt network
     * @return patch port number
     */
    public static PortNumber tunnelToTenantPort(DeviceService deviceService,
                             KubevirtNode node, KubevirtNetwork network) {
        if (network.segmentId() == null) {
            return null;
        }

        if (node.tunBridge() == null) {
            return null;
        }

        String tunToTenantPortName = TUNNEL_TO_TENANT_PREFIX + segmentIdHex(network.segmentId());
        return portNumber(deviceService, node.tunBridge(), tunToTenantPortName);
    }

    /**
     * Obtains the tunnel port number of the given node.
     *
     * @param node    kubevirt node
     * @param network kubevirt network
     * @return tunnel port number
     */
    public static PortNumber tunnelPort(KubevirtNode node, KubevirtNetwork network) {
        if (network.segmentId() == null) {
            return null;
        }

        if (node.tunBridge() == null) {
            return null;
        }

        switch (network.type()) {
            case VXLAN:
                return node.vxlanPort();
            case GRE:
                return node.grePort();
            case GENEVE:
                return node.genevePort();
            case STT:
                return node.sttPort();
            case FLAT:
            case VLAN:
            default:
                // do nothing
                return null;
        }
    }

    public static String parseResourceName(String resource) {
        JsonObject json = JsonObject.readFrom(resource);
        JsonObject metadata = json.get("metadata").asObject();
        return metadata != null ? metadata.get("name").asString() : "";
    }

    public static PortNumber portNumber(DeviceService deviceService, DeviceId deviceId, String portName) {
        Port port = deviceService.getPorts(deviceId).stream()
                .filter(p -> p.isEnabled() &&
                        Objects.equals(p.annotations().value(PORT_NAME), portName))
                .findAny().orElse(null);
        return port != null ? port.number() : null;
    }

    /**
     * Returns the gateway node for the specified kubevirt router.
     * Among gateways, only one gateway would act as a gateway per perter.
     * Currently gateway node is selected based on modulo operation with router hashcode.
     *
     * @param nodeService kubevirt node service
     * @param router      kubevirt router
     * @return elected gateway node
     */
    public static KubevirtNode gatewayNodeForSpecifiedRouter(KubevirtNodeService nodeService,
                                                             KubevirtRouter router) {
        //TODO: enhance election logic for a better load balancing

        int numOfGateways = nodeService.completeNodes(GATEWAY).size();
        if (numOfGateways == 0) {
            return null;
        }
        return (KubevirtNode) nodeService.completeNodes(GATEWAY)
                .toArray()[Math.floorMod(router.hashCode(), numOfGateways)];
    }

    /**
     * Returns the gateway node for the specified kubernetes external lb.
     * Among gateways, only one gateway would act as a gateway per external lb.
     * Currently gateway node is selected based on modulo operation with external lb hashcode.
     *
     * @param nodeService kubevirt node service
     * @param externalLb kubernetes external lb
     * @return elected gateway node
     */
    public static KubevirtNode gatewayNodeForSpecifiedService(KubevirtNodeService nodeService,
                                                              KubernetesExternalLb externalLb) {
        //TODO: enhance election logic for a better load balancing
        try {
            int numOfGateways = nodeService.completeExternalLbGatewayNodes().size();
            if (numOfGateways == 0) {
                return null;
            }

            return (KubevirtNode) nodeService.completeExternalLbGatewayNodes()
                    .toArray()[Math.floorMod(externalLb.hashCode(), numOfGateways)];
        } catch (IndexOutOfBoundsException e) {
            log.error("IndexOutOfBoundsException occurred {}", e.toString());
            return null;
        }
    }

    /**
     * Returns whether a mac address in kubernetes external lb config is updated.
     *
     * @param externalLbConfig kubernetes external lb config
     * @return true if a mac address is added
     */
    public static boolean configMapUpdated(KubernetesExternalLbConfig externalLbConfig) {
        if (externalLbConfig == null) {
            return false;
        }

        return externalLbConfig.configName() != null && externalLbConfig.globalIpRange() != null &&
                externalLbConfig.loadBalancerGwIp() != null && externalLbConfig.loadBalancerGwMac() != null;
    }


    /**
     * Returns the worker node for the specified kubernetes external lb.
     * Among worker nodes, only one worker would serve the traffic from and to the gateway.
     * Currently worker node is selected based on modulo operation with external lb hashcode.
     *
     * @param nodeService kubevirt node service
     * @param externalLb kubernetes external lb
     * @return elected worker node
     */
    public static KubevirtNode workerNodeForSpecifiedService(KubevirtNodeService nodeService,
                                                             KubernetesExternalLb externalLb) {
        //TODO: enhance election logic for a better load balancing

        int numOfWorkers = nodeService.completeNodes(WORKER).size();
        if (numOfWorkers == 0) {
            return null;
        }

        return (KubevirtNode) nodeService.completeNodes(WORKER)
                .toArray()[externalLb.hashCode() % numOfWorkers];
    }

    /**
     * Returns the mac address of the router.
     *
     * @param router kubevirt router
     * @return macc address of the router
     */
    public static MacAddress getRouterMacAddress(KubevirtRouter router) {
        if (router.mac() == null) {
            log.warn("Failed to get mac address of router {}", router.name());
        }

        return router.mac();
    }

    /**
     * Returns the snat ip address with specified router.
     *
     * @param routerService     router service
     * @param internalNetworkId internal network id which is associated with the router
     * @return snat ip address if exist, null otherwise
     */
    public static IpAddress getRouterSnatIpAddress(KubevirtRouterService routerService,
                                                    String internalNetworkId) {
        KubevirtRouter router = routerService.routers().stream()
                .filter(r -> r.internal().contains(internalNetworkId))
                .findAny().orElse(null);

        if (router == null) {
            return null;
        }

        String routerSnatIp = router.external().keySet().stream().findAny().orElse(null);

        if (routerSnatIp == null) {
            return null;
        }

        return Ip4Address.valueOf(routerSnatIp);
    }

    /**
     * Returns the kubevirt router with specified kubevirt port.
     *
     * @param routerService kubevirt router service
     * @param kubevirtPort  kubevirt port
     * @return kubevirt router
     */
    public static KubevirtRouter getRouterForKubevirtPort(KubevirtRouterService routerService,
                                                          KubevirtPort kubevirtPort) {
        if (kubevirtPort.ipAddress() != null) {
            return routerService.routers().stream()
                    .filter(r -> r.internal().contains(kubevirtPort.networkId()))
                    .findAny().orElse(null);
        }
        return null;
    }

    /**
     * Returns the kubevirt router with specified kubevirt network.
     *
     * @param routerService kubevirt router service
     * @param kubevirtNetwork kubevirt network
     * @return kubevirt router
     */
    public static KubevirtRouter getRouterForKubevirtNetwork(KubevirtRouterService routerService,
                                                             KubevirtNetwork kubevirtNetwork) {
        return routerService.routers().stream()
                .filter(router -> router.internal().contains(kubevirtNetwork.networkId()))
                .findAny().orElse(null);
    }

    /**
     * Returns the external patch port number with specified gateway.
     *
     * @param deviceService device service
     * @param gatewayNode gateway node
     * @return external patch port number
     */
    public static PortNumber externalPatchPortNum(DeviceService deviceService, KubevirtNode gatewayNode) {
        String gatewayBridgeName = gatewayNode.gatewayBridgeName();
        if (gatewayBridgeName == null) {
            log.warn("No external interface is attached to gateway {}", gatewayNode.hostname());
            return null;
        }

        String patchPortName = "int-to-" + gatewayBridgeName;
        Port port = deviceService.getPorts(gatewayNode.intgBridge()).stream()
                .filter(p -> p.isEnabled() &&
                        Objects.equals(p.annotations().value(PORT_NAME), patchPortName))
                .findAny().orElse(null);

        return port != null ? port.number() : null;
    }

    /**
     * Returns the external lb patch port number with specified gateway.
     *
     * @param deviceService device service
     * @param gateway gateway node
     * @return external lb bridge patch port number
     */
    public static PortNumber elbPatchPortNum(DeviceService deviceService, KubevirtNode gateway) {
        KubernetesExternalLbInterface kubernetesExternalLbInterface =
                gateway.kubernetesExternalLbInterface();

        if (kubernetesExternalLbInterface == null) {
            log.warn("No elb interface is attached to gateway {}", gateway.hostname());
            return null;
        }

        String elbBridgeName = kubernetesExternalLbInterface.externalLbBridgeName();

        String patchPortName = "int-to-" + elbBridgeName;

        Port port = deviceService.getPorts(gateway.intgBridge()).stream()
                .filter(p -> p.isEnabled() &&
                        Objects.equals(p.annotations().value(PORT_NAME), patchPortName))
                .findAny().orElse(null);

        return port != null ? port.number() : null;
    }

    /**
     * Returns the external lb patch port Mac with specified gateway.
     *
     * @param deviceService device service
     * @param gateway gateway node
     * @return external lb bridge patch Mac Address
     */
    public static MacAddress kubernetesElbMac(DeviceService deviceService, KubevirtNode gateway) {

        KubernetesExternalLbInterface kubernetesExternalLbInterface =
                gateway.kubernetesExternalLbInterface();

        if (kubernetesExternalLbInterface == null) {
            log.warn("No elb interface is attached to gateway {}", gateway.hostname());
            return null;
        }

        String elbBridgeName = kubernetesExternalLbInterface.externalLbBridgeName();

        String patchPortName = "int-to-" + elbBridgeName;

        Port port = deviceService.getPorts(gateway.intgBridge()).stream()
                .filter(p -> p.isEnabled() &&
                        Objects.equals(p.annotations().value(PORT_NAME), patchPortName))
                .findAny().orElse(null);

        if (port == null) {
            return null;
        }

        String portMacStr = port.annotations().value(PORT_MAC);

        if (portMacStr == null) {
            return null;
        }

        return MacAddress.valueOf(portMacStr);
    }

    /**
     * Returns the kubevirt external network with specified router.
     *
     * @param networkService kubevirt network service
     * @param router kubevirt router
     * @return external network
     */
    public static KubevirtNetwork getExternalNetworkByRouter(KubevirtNetworkService networkService,
                                                             KubevirtRouter router) {
        String networkId = router.external().values().stream().findAny().orElse(null);
        if (networkId == null) {
            return null;
        }

        return networkService.network(networkId);
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
     * Builds a GARP packet using the given source MAC and source IP address.
     *
     * @param srcMac source MAC address
     * @param srcIp  source IP address
     * @return GARP packet
     */
    public static Ethernet buildGarpPacket(MacAddress srcMac, IpAddress srcIp) {
        if (srcMac == null || srcIp == null) {
            return null;
        }

        Ethernet ethernet = new Ethernet();
        ethernet.setDestinationMACAddress(MacAddress.BROADCAST);
        ethernet.setSourceMACAddress(srcMac);
        ethernet.setEtherType(Ethernet.TYPE_ARP);

        ARP arp = new ARP();
        arp.setOpCode(ARP.OP_REPLY);
        arp.setProtocolType(ARP.PROTO_TYPE_IP);
        arp.setHardwareType(ARP.HW_TYPE_ETHERNET);

        arp.setProtocolAddressLength((byte) Ip4Address.BYTE_LENGTH);
        arp.setHardwareAddressLength((byte) Ethernet.DATALAYER_ADDRESS_LENGTH);

        arp.setSenderHardwareAddress(srcMac.toBytes());
        arp.setTargetHardwareAddress(MacAddress.BROADCAST.toBytes());

        arp.setSenderProtocolAddress(srcIp.toOctets());
        arp.setTargetProtocolAddress(srcIp.toOctets());

        ethernet.setPayload(arp);

        return ethernet;
    }

    /**
     * Obtains flow group key from the given id.
     *
     * @param groupId flow group identifier
     * @return flow group key
     */
    public static GroupKey getGroupKey(int groupId) {
        return new DefaultGroupKey((Integer.toString(groupId)).getBytes());
    }

    /**
     * Obtains load balancer set from the given router.
     *
     * @param router kubevirt router
     * @param lbService kubevirt loadbalancer service
     * @return loadbalancer set
     */
    public static Set<KubevirtLoadBalancer> getLoadBalancerSetForRouter(KubevirtRouter router,
                                                                        KubevirtLoadBalancerService lbService) {

        return lbService.loadBalancers().stream()
                .filter(lb -> router.internal().contains(lb.networkId()))
                .collect(Collectors.toSet());
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
     * Returns the kubevirt node from the node.
     *
     * @param node a raw node object returned from a k8s client
     * @return kubevirt node
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
            if (MASTER.name().equalsIgnoreCase(role)) {
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
        String calicoVxlanIpStr = annots.get(CALICO_VXLAN_TUNNEL_ADDR);
        Set<KubevirtPhyInterface> phys = new HashSet<>();
        String gatewayBridgeName = null;
        boolean vxlanInUse = calicoVxlanIpStr != null;

        try {
            if (physnetConfig != null) {
                JsonArray configJson = JsonArray.readFrom(physnetConfig);

                for (int i = 0; i < configJson.size(); i++) {
                    JsonObject object = configJson.get(i).asObject();
                    String network = object.get(NETWORK_KEY).asString();
                    String intf = object.get(INTERFACE_KEY).asString();
                    JsonValue jsonKaasElbs = object.get(KAAS_ELB_GWS_KEY);

                    if (network != null && intf != null) {
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
            }

            if (tunnelConfig != null) {
                JsonNode jsonNode = new ObjectMapper().readTree(tunnelConfig);
                dataIp = IpAddress.valueOf(jsonNode.get("ip").asText());
            } else if (dataIpStr != null) {
                dataIp = IpAddress.valueOf(dataIpStr);
            }

            if (gatewayConfig != null) {
                JsonNode jsonNode = new ObjectMapper().readTree(gatewayConfig);

                nodeType = GATEWAY;
                gatewayBridgeName = jsonNode.get(GATEWAY_BRIDGE_NAME).asText();
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to parse physnet config or gateway config object", e);
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
                .kernelVersion(kernelVersion)
                .vxlanInUse(vxlanInUse)
                .build();
    }

    /**
     * Parses kubevirt network resource.
     *
     * @param resource kubevirt network resource string
     * @return kubevirt network object
     */
    public static KubevirtNetwork parseKubevirtNetwork(String resource) {
        JsonObject json = JsonObject.readFrom(resource);
        String name = parseResourceName(resource);
        JsonObject annots = json.get("metadata").asObject().get("annotations").asObject();
        if (annots.get(NETWORK_CONFIG) == null) {
            // SR-IOV network does not contain network-config field
            return null;
        }
        String networkConfig = annots.get(NETWORK_CONFIG).asString();
        if (networkConfig != null) {
            KubevirtNetwork.Builder builder = DefaultKubevirtNetwork.builder();

            JsonObject configJson = JsonObject.readFrom(networkConfig);
            String id = configJson.get(NETWORK_ID).asString();
            String type = configJson.get(TYPE).asString().toUpperCase(Locale.ROOT);
            Integer mtu = configJson.get(MTU).asInt();
            String gatewayIp = configJson.getString(GATEWAY_IP, "");
            boolean defaultRoute = configJson.getBoolean(DEFAULT_ROUTE, false);

            if (!type.equalsIgnoreCase(FLAT.name())) {
                builder.segmentId(configJson.getString(SEGMENT_ID, ""));
            } else {
                builder.physnetName(configJson.getString(PHYSNET_NAME, ""));
            }

            String cidr = configJson.getString(CIDR, "");

            JsonObject poolJson = configJson.get(IP_POOL).asObject();
            if (poolJson != null) {
                String start = poolJson.getString(START, "");
                String end = poolJson.getString(END, "");
                builder.ipPool(new KubevirtIpPool(
                        IpAddress.valueOf(start), IpAddress.valueOf(end)));
            }

            if (configJson.get(HOST_ROUTES) != null) {
                JsonArray routesJson = configJson.get(HOST_ROUTES).asArray();
                Set<KubevirtHostRoute> hostRoutes = new HashSet<>();
                if (routesJson != null) {
                    for (int i = 0; i < routesJson.size(); i++) {
                        JsonObject route = routesJson.get(i).asObject();
                        String destinationStr = route.getString(DESTINATION, "");
                        String nexthopStr = route.getString(NEXTHOP, "");

                        if (StringUtils.isNotEmpty(destinationStr) &&
                                StringUtils.isNotEmpty(nexthopStr)) {
                            hostRoutes.add(new KubevirtHostRoute(
                                    IpPrefix.valueOf(destinationStr),
                                    IpAddress.valueOf(nexthopStr)));
                        }
                    }
                }
                builder.hostRoutes(hostRoutes);
            }

            if (configJson.get(DNSES) != null) {
                JsonArray dnsesJson = configJson.get(DNSES).asArray();
                Set<IpAddress> dnses = new HashSet<>();
                if (dnsesJson != null) {
                    for (int i = 0; i < dnsesJson.size(); i++) {
                        String dns = dnsesJson.get(i).asString();
                        if (StringUtils.isNotEmpty(dns)) {
                            dnses.add(IpAddress.valueOf(dns));
                        }
                    }
                }
                builder.dnses(dnses);
            }

            builder.networkId(id).name(name).type(KubevirtNetwork.Type.valueOf(type))
                    .mtu(mtu).gatewayIp(IpAddress.valueOf(gatewayIp))
                    .defaultRoute(defaultRoute).cidr(cidr);

            return builder.build();
        }
        return null;
    }
}
