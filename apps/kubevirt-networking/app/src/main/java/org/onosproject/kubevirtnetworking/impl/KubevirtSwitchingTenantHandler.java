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
package org.onosproject.kubevirtnetworking.impl;

import org.apache.commons.lang.StringUtils;
import org.onlab.packet.Ethernet;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.IpPrefix;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtFlowRuleService;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkService;
import org.onosproject.kubevirtnetworking.api.KubevirtPort;
import org.onosproject.kubevirtnetworking.api.KubevirtPortEvent;
import org.onosproject.kubevirtnetworking.api.KubevirtPortListener;
import org.onosproject.kubevirtnetworking.api.KubevirtPortService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeEvent;
import org.onosproject.kubevirtnode.api.KubevirtNodeListener;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.Device;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.DriverService;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.api.Constants.PRIORITY_TUNNEL_RULE;
import static org.onosproject.kubevirtnetworking.api.Constants.TUNNEL_DEFAULT_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.TUNNEL_TO_TENANT_PREFIX;
import static org.onosproject.kubevirtnetworking.api.KubevirtNetwork.Type.FLAT;
import static org.onosproject.kubevirtnetworking.api.KubevirtNetwork.Type.VLAN;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.tunnelPort;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.tunnelToTenantPort;
import static org.onosproject.kubevirtnetworking.util.RulePopulatorUtil.buildExtension;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.MASTER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.onosproject.net.AnnotationKeys.PORT_NAME;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Populates switching flow rules on OVS for the tenant network (overlay).
 */
@Component(immediate = true)
public class KubevirtSwitchingTenantHandler {
    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected MastershipService mastershipService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DriverService driverService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ClusterService clusterService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LeadershipService leadershipService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtFlowRuleService flowRuleService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeService kubevirtNodeService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNetworkService kubevirtNetworkService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtPortService kubevirtPortService;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler"));

    private final InternalKubevirtPortListener kubevirtPortListener =
            new InternalKubevirtPortListener();
    private final InternalKubevirtNodeListener kubevirtNodeListener =
            new InternalKubevirtNodeListener();

    private final DeviceListener bridgeListener = new InternalBridgeListener();

    private ApplicationId appId;
    private NodeId localNodeId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(KUBEVIRT_NETWORKING_APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());
        kubevirtPortService.addListener(kubevirtPortListener);
        kubevirtNodeService.addListener(kubevirtNodeListener);
        deviceService.addListener(bridgeListener);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        deviceService.removeListener(bridgeListener);
        kubevirtPortService.removeListener(kubevirtPortListener);
        kubevirtNodeService.removeListener(kubevirtNodeListener);
        leadershipService.withdraw(appId.name());
        eventExecutor.shutdown();

        log.info("Stopped");
    }

    private void setIngressRules(KubevirtNode node, boolean install) {
        for (KubevirtNetwork network : kubevirtNetworkService.tenantNetworks()) {

            if (node == null || node.type() != WORKER) {
                return;
            }

            if (network.type() == FLAT || network.type() == VLAN) {
                return;
            }

            if (network.segmentId() == null) {
                return;
            }

            KubevirtNode updatedNode = kubevirtNodeService.node(node.hostname());
            PortNumber patchPortNumber = tunnelToTenantPort(deviceService, updatedNode, network);
            if (patchPortNumber == null) {
                log.warn("Patch port of tenant {} is not ready for node {}",
                        network.segmentId(), updatedNode.hostname());
                continue;
            }

            TrafficSelector.Builder sBuilder = DefaultTrafficSelector.builder()
                    .matchTunnelId(Long.parseLong(network.segmentId()));

            TrafficTreatment.Builder tBuilder = DefaultTrafficTreatment.builder()
                    .setOutput(patchPortNumber);

            flowRuleService.setRule(
                    appId,
                    node.tunBridge(),
                    sBuilder.build(),
                    tBuilder.build(),
                    PRIORITY_TUNNEL_RULE,
                    TUNNEL_DEFAULT_TABLE,
                    install);

            log.debug("Set ingress rules for segment ID {}", network.segmentId());
        }
    }

    private void setEgressRules(KubevirtPort port, boolean install) {
        if (port.ipAddress() == null) {
            return;
        }

        KubevirtNetwork network = kubevirtNetworkService.network(port.networkId());

        if (network == null) {
            return;
        }

        if (network.type() == FLAT || network.type() == VLAN) {
            return;
        }

        if (network.segmentId() == null) {
            return;
        }

        KubevirtNode localNode = kubevirtNodeService.node(port.deviceId());

        if (localNode == null || localNode.type() == MASTER) {
            return;
        }

        for (KubevirtNode remoteNode : kubevirtNodeService.completeNodes(WORKER)) {
            if (remoteNode.hostname().equals(localNode.hostname())) {
                continue;
            }

            setEgressRule(port, network, localNode, remoteNode, install);
        }

        log.debug("Set egress rules for instance {}, segment ID {}",
                port.ipAddress(), network.segmentId());
    }

    /**
     * Installs the egress rules for the given port on the given remote node
     * only. Used to (re)program a single node right after it completed its
     * bootstrap, without touching the rest of the tunnel mesh.
     *
     * @param port       kubevirt port to reach
     * @param remoteNode node whose tunnel bridge is programmed
     * @param install    installation flag
     */
    private void setEgressRulesOnRemote(KubevirtPort port, KubevirtNode remoteNode, boolean install) {
        if (port.ipAddress() == null) {
            return;
        }

        KubevirtNetwork network = kubevirtNetworkService.network(port.networkId());

        if (network == null) {
            return;
        }

        if (network.type() == FLAT || network.type() == VLAN) {
            return;
        }

        if (network.segmentId() == null) {
            return;
        }

        KubevirtNode localNode = kubevirtNodeService.node(port.deviceId());

        if (localNode == null || localNode.type() == MASTER) {
            return;
        }

        if (remoteNode.hostname().equals(localNode.hostname())) {
            return;
        }

        setEgressRule(port, network, localNode, remoteNode, install);
    }

    private void setEgressRule(KubevirtPort port, KubevirtNetwork network,
                               KubevirtNode localNode, KubevirtNode remoteNode,
                               boolean install) {
        PortNumber patchPortNumber = tunnelToTenantPort(deviceService, remoteNode, network);
        if (patchPortNumber == null) {
            log.warn("Patch port of tenant {} is not ready for node {}",
                    network.segmentId(), remoteNode.hostname());
            return;
        }

        PortNumber tunnelPortNumber = tunnelPort(remoteNode, network);
        if (tunnelPortNumber == null) {
            log.warn("Tunnel port of tenant {} is not ready for node {}",
                    network.segmentId(), remoteNode.hostname());
            return;
        }

        TrafficSelector.Builder sIpBuilder = DefaultTrafficSelector.builder()
                .matchInPort(patchPortNumber)
                .matchEthType(Ethernet.TYPE_IPV4)
                .matchIPDst(IpPrefix.valueOf(port.ipAddress(), 32));

        TrafficSelector.Builder sArpBuilder = DefaultTrafficSelector.builder()
                .matchInPort(patchPortNumber)
                .matchEthType(Ethernet.TYPE_ARP)
                .matchArpTpa(Ip4Address.valueOf(port.ipAddress().toString()));

        TrafficTreatment.Builder tBuilder = DefaultTrafficTreatment.builder()
                .setTunnelId(Long.parseLong(network.segmentId()))
                .extension(buildExtension(
                        deviceService,
                        remoteNode.tunBridge(),
                        localNode.dataIp().getIp4Address()),
                        remoteNode.tunBridge())
                .setOutput(tunnelPortNumber);

        flowRuleService.setRule(
                appId,
                remoteNode.tunBridge(),
                sIpBuilder.build(),
                tBuilder.build(),
                PRIORITY_TUNNEL_RULE,
                TUNNEL_DEFAULT_TABLE,
                install);

        flowRuleService.setRule(
                appId,
                remoteNode.tunBridge(),
                sArpBuilder.build(),
                tBuilder.build(),
                PRIORITY_TUNNEL_RULE,
                TUNNEL_DEFAULT_TABLE,
                install);
    }

    private class InternalKubevirtNodeListener implements KubevirtNodeListener {

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtNodeEvent event) {

            switch (event.type()) {
                case KUBEVIRT_NODE_COMPLETE:
                    eventExecutor.execute(() -> processNodeCompletion(event.subject()));
                    break;
                case KUBEVIRT_NODE_INCOMPLETE:
                default:
                    // do nothing
                    break;
            }
        }

        private void processNodeCompletion(KubevirtNode node) {
            if (!isRelevantHelper()) {
                return;
            }

            setIngressRules(node, true);
            kubevirtPortService.ports().stream()
                    .filter(port -> node.equals(kubevirtNodeService.node(port.deviceId())))
                    .forEach(port -> {
                        setEgressRules(port, true);
                    });

            // Also (re)program THIS node with the egress rules of the ports
            // hosted by other nodes. When several nodes bootstrap concurrently,
            // this node may not have been COMPLETE yet at the moment those
            // ports were processed, so the rules pointing at them are missing
            // here; without this mirror pass the tunnel mesh stays partially
            // programmed regardless of the completion order.
            if (node.type() == WORKER) {
                kubevirtPortService.ports().stream()
                        .filter(port -> !node.equals(kubevirtNodeService.node(port.deviceId())))
                        .forEach(port -> setEgressRulesOnRemote(port, node, true));
            }
        }
    }

    private class InternalBridgeListener implements DeviceListener {

        @Override
        public boolean isRelevant(DeviceEvent event) {
            return event.subject().type() == Device.Type.SWITCH;
        }

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(DeviceEvent event) {
            Device device = event.subject();
            Port port = event.port();

            switch (event.type()) {
                case PORT_ADDED:
                    eventExecutor.execute(() -> {
                        if (!isRelevantHelper()) {
                            return;
                        }

                        setIngressRules(device, port, true);
                    });
                    break;
                case PORT_REMOVED:
                    eventExecutor.execute(() -> {
                        if (!isRelevantHelper()) {
                            return;
                        }

                        setIngressRules(device, port, false);
                    });
                    break;
                default:
                    // do nothing
                    break;
            }
        }

        private void setIngressRules(Device device, Port port, boolean install) {
            boolean foundTunBridge = false;

            for (KubevirtNode node : kubevirtNodeService.nodes()) {
                if (device.id().equals(node.tunBridge())) {
                    foundTunBridge = true;
                }
            }

            if (!foundTunBridge) {
                return;
            }

            String portName = port.annotations().value(PORT_NAME);
            if (!StringUtils.startsWithIgnoreCase(portName, TUNNEL_TO_TENANT_PREFIX)) {
                return;
            }

            Pattern pattern = Pattern.compile("(?i)[0-9a-f]+");
            Matcher matcher = pattern.matcher(portName);

            // Find the first occurrence of a hexadecimal digit sequence
            if (matcher.find()) {
                // Convert the hexadecimal digits to an integer
                int value = Integer.parseInt(matcher.group(), 16);

                TrafficSelector.Builder sBuilder = DefaultTrafficSelector.builder()
                        .matchTunnelId(Long.parseLong(Integer.toString(value)));

                TrafficTreatment.Builder tBuilder = DefaultTrafficTreatment.builder()
                        .setOutput(port.number());

                flowRuleService.setRule(
                        appId,
                        device.id(),
                        sBuilder.build(),
                        tBuilder.build(),
                        PRIORITY_TUNNEL_RULE,
                        TUNNEL_DEFAULT_TABLE,
                        install);

                log.debug("Set ingress rules for segment ID {}", value);
            }
        }
    }
    private class InternalKubevirtPortListener implements KubevirtPortListener {

        @Override
        public boolean isRelevant(KubevirtPortEvent event) {
            return event.subject().deviceId() != null;
        }

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtPortEvent event) {

            switch (event.type()) {
                case KUBEVIRT_PORT_UPDATED:
                    eventExecutor.execute(() -> processPortUpdate(event.subject()));
                    break;
                case KUBEVIRT_PORT_REMOVED:
                    eventExecutor.execute(() -> processPortRemoval(event.subject()));
                    break;
                case KUBEVIRT_PORT_MIGRATED:
                    eventExecutor.execute(() -> processPortUpdate(event.subject()));
                    eventExecutor.execute(() -> processPortRemoval(event.oldSubject()));
                    break;
                default:
                    // do nothing
                    break;
            }
        }

        private void processPortUpdate(KubevirtPort port) {
            if (!isRelevantHelper()) {
                return;
            }

            setEgressRules(port, true);
        }

        private void processPortRemoval(KubevirtPort port) {
            if (!isRelevantHelper()) {
                return;
            }

            setEgressRules(port, false);
        }
    }
}
