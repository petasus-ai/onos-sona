/*
 * Copyright 2022-present Open Networking Foundation
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

import org.onlab.packet.ARP;
import org.onlab.packet.EthType;
import org.onlab.packet.Ethernet;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtFlowRuleService;
import org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigAdminService;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigListener;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbInterface;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeAdminService;
import org.onosproject.kubevirtnode.api.KubevirtNodeEvent;
import org.onosproject.kubevirtnode.api.KubevirtNodeListener;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.packet.DefaultOutboundPacket;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.GW_ENTRY_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBERNETES_EXTERNAL_LB_FAKE_MAC;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.api.Constants.PRIORITY_ARP_GATEWAY_RULE;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.kubernetesElbMac;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Handles arp packets related to the kubernetes external loadbalancer handler.
 */
@Component(immediate = true)
public class KubernetesExternalLbArpHandler {
    protected final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LeadershipService leadershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubernetesExternalLbConfigAdminService externalLbConfigAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeAdminService nodeAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtFlowRuleService kubevirtFlowRuleService;

    private static final IpAddress NON_ROUTABLE_META_ADDRESS = IpAddress.valueOf("0.0.0.0");
    private final Timer externalLbGwTimer = new Timer("kubernetes-external-lb-gateway");
    private final Timer externalLbIntfGwTimer = new Timer("kubernetes-external-lb-intf-gateway");
    private static final long SECONDS = 1000L;
    private static final long INITIAL_DELAY = 5 * SECONDS;
    private static final long TASK_PERIOD = 60 * SECONDS;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler"));

    private final PacketProcessor packetProcessor = new InternalPacketProcessor();
    private final InternalKubernetesExternalLbConfigListener
            lbConfigListener = new InternalKubernetesExternalLbConfigListener();

    private final InternalNodeEventListener
            nodeEventListener = new InternalNodeEventListener();


    private ApplicationId appId;
    private NodeId localNodeId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(KUBEVIRT_NETWORKING_APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());

        packetService.addProcessor(packetProcessor, PacketProcessor.director(1));
        externalLbConfigAdminService.addListener(lbConfigListener);
        nodeAdminService.addListener(nodeEventListener);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        leadershipService.withdraw(appId.name());
        packetService.removeProcessor(packetProcessor);
        externalLbConfigAdminService.removeListener(lbConfigListener);
        nodeAdminService.removeListener(nodeEventListener);

        eventExecutor.shutdown();

        log.info("Stopped");
    }

    private class InternalKubernetesExternalLbConfigListener
            implements KubernetesExternalLbConfigListener {

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubernetesExternalLbConfigEvent event) {
            switch (event.type()) {
                case KUBERNETES_EXTERNAL_LB_CONFIG_CREATED:
                case KUBERNETES_EXTERNAL_LB_CONFIG_UPDATED:
                    eventExecutor.execute(() -> processConfigCreatedOrUpdated(event.subject()));
                    break;
                case KUBERNETES_EXTERNAL_LB_CONFIG_REMOVED:
                default:
                    //do nothing
                    break;
            }
        }
        private void processConfigCreatedOrUpdated(KubernetesExternalLbConfig config) {
            if (!isRelevantHelper()) {
                return;
            }

            if (config.loadBalancerGwMac() != null) {
                return;
            }

            processKubernetesExternalLbConfigMacLearning(config);
        }
    }

    private class InternalNodeEventListener implements KubevirtNodeListener {

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
                case KUBEVIRT_NODE_UPDATED:
                case KUBEVIRT_NODE_REMOVED:
                    break;

                default:
                    // do nothing
                    break;
            }
        }

        private void processNodeCompletion(KubevirtNode node) {
            if (!isRelevantHelper()) {
                return;
            }
            if (node.type().equals(GATEWAY)) {
                KubernetesExternalLbInterface externalLbInterface =
                        node.kubernetesExternalLbInterface();

                if (externalLbInterface != null && externalLbInterface.externalLbGwMac() == null) {
                    processKubernetesExternalLbIntfGwMacLearningForGatewayNode(node);
                }

                KubernetesExternalLbConfig config =
                        externalLbConfigAdminService.lbConfigs().stream().findAny().orElse(null);

                if (config != null && config.loadBalancerGwMac() == null) {
                    processKubernetesExternalLbConfigMacLearning(config);
                }
            }
        }
    }

    private void processKubernetesExternalLbConfigMacLearning(KubernetesExternalLbConfig config) {
        nodeAdminService.completeExternalLbGatewayNodes().forEach(gateway -> {
            setRuleArpRequestToController(config.loadBalancerGwIp(),
                    KUBERNETES_EXTERNAL_LB_FAKE_MAC, gateway, true);
        });

        KubevirtNode gateway = nodeAdminService.completeExternalLbGatewayNodes()
                .stream().findAny().orElse(null);
        if (gateway == null) {
            return;
        }
        PortNumber externalPatchPortNum = KubevirtNetworkingUtil.externalPatchPortNum(deviceService, gateway);

        if (externalPatchPortNum == null) {
            log.warn("processKubernetesExternalLbConfigMacLearning" +
                            " called but there's no external patchPort for {}. Stop this task.",
                    gateway);
            return;
        }

        retrievePeerMac(NON_ROUTABLE_META_ADDRESS, KUBERNETES_EXTERNAL_LB_FAKE_MAC,
                config.loadBalancerGwIp(), gateway, externalPatchPortNum);
        checkKubernetesExternalLbConfigMacRetrieved(config, gateway);
    }


    private void processKubernetesExternalLbIntfGwMacLearningForGatewayNode(KubevirtNode gatewayNode) {

        MacAddress elbIntfMac = kubernetesElbMac(deviceService, gatewayNode);
        if (elbIntfMac == null) {
            log.warn("processKubernetesExternalLbGwMacLearningForNode called but elbIntfMac is null. Stop this task.");
            return;
        }

        KubernetesExternalLbInterface externalLbInterface = gatewayNode.kubernetesExternalLbInterface();

        setRuleArpRequestToController(externalLbInterface.externalLbGwIp(), elbIntfMac,
                gatewayNode, true);

        PortNumber elbPatchPortNum = KubevirtNetworkingUtil.elbPatchPortNum(deviceService, gatewayNode);

        if (elbPatchPortNum == null) {
            log.warn("processKubernetesExternalLbIntfGwMacLearningForGatewayNode" +
                            " called but there's no elb patchPort for {}. Stop this task.",
                    gatewayNode);
            return;
        }

        retrievePeerMac(externalLbInterface.externalLbIp(), elbIntfMac,
                externalLbInterface.externalLbGwIp(), gatewayNode, elbPatchPortNum);

        checkKubernetesExternalLbIntfGwMacRetrieved(gatewayNode);
    }

    private void checkKubernetesExternalLbIntfGwMacRetrieved(KubevirtNode gateway) {
        KubernetesExternalLbInterface externalLbInterface = gateway.kubernetesExternalLbInterface();

        MacAddress elbIntfMac = kubernetesElbMac(deviceService, gateway);
        if (elbIntfMac == null) {
            log.warn("setDownstreamRules called but elbIntfMac is null. Stop this task.");
            return;
        }

        KubernetexExternalLbIntfTimerTask task = new KubernetexExternalLbIntfTimerTask(
                externalLbInterface.externalLbIp(), elbIntfMac,
                externalLbInterface.externalLbGwIp(), gateway);

        externalLbIntfGwTimer.schedule(task, INITIAL_DELAY, TASK_PERIOD);
    }

    private void checkKubernetesExternalLbConfigMacRetrieved(KubernetesExternalLbConfig config,
                                                             KubevirtNode gateway) {
        KubernetesExternalLbConfigTimerTask task = new KubernetesExternalLbConfigTimerTask(
                IpAddress.valueOf("0.0.0.0"), KUBERNETES_EXTERNAL_LB_FAKE_MAC,
                config.loadBalancerGwIp(), gateway);

        externalLbGwTimer.schedule(task, INITIAL_DELAY, TASK_PERIOD);
    }


    private void setRuleArpRequestToController(IpAddress targetIpAddress,
                                               MacAddress dstMac,
                                               KubevirtNode gatewayNode,
                                               boolean install) {
        if (targetIpAddress == null || dstMac == null || gatewayNode == null) {
            return;
        }

        TrafficSelector selector = DefaultTrafficSelector.builder()
                .matchEthType(EthType.EtherType.ARP.ethType().toShort())
                .matchArpOp(ARP.OP_REPLY)
                .matchArpSpa(targetIpAddress.getIp4Address())
                .matchArpTha(dstMac)
                .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .punt()
                .build();

        kubevirtFlowRuleService.setRule(
                appId,
                gatewayNode.intgBridge(),
                selector,
                treatment,
                PRIORITY_ARP_GATEWAY_RULE,
                GW_ENTRY_TABLE,
                install
        );
    }


    private void retrievePeerMac(IpAddress srcIp, MacAddress srcMac,
                                 IpAddress peerIp, KubevirtNode gatewayNode,
                                 PortNumber portNumber) {
        log.debug("Sending ARP request to the peer {} to retrieve the MAC address.",
                peerIp.getIp4Address().toString());

        Ethernet ethRequest = ARP.buildArpRequest(srcMac.toBytes(),
                srcIp.toOctets(),
                peerIp.toOctets(), VlanId.NO_VID);

        if (gatewayNode == null) {
            log.warn("retrievePeerMac called but there's no gateway node for {}. Stop this task.",
                    gatewayNode);
            return;
        }

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber)
                .build();

        packetService.emit(new DefaultOutboundPacket(
                gatewayNode.intgBridge(),
                treatment,
                ByteBuffer.wrap(ethRequest.serialize())));

    }

    private class KubernetesExternalLbConfigTimerTask extends TimerTask {
        private final IpAddress srcIp;
        private final MacAddress srcMac;
        private final IpAddress peerIp;
        private final KubevirtNode gatewayNode;

        public KubernetesExternalLbConfigTimerTask(IpAddress srcIp, MacAddress srcMac,
                                 IpAddress peerIp, KubevirtNode gatewayNode) {
            this.srcIp = srcIp;
            this.srcMac = srcMac;
            this.peerIp = peerIp;
            this.gatewayNode = gatewayNode;
        }

        @Override
        public void run() {
            KubernetesExternalLbConfig config =
                    externalLbConfigAdminService.lbConfigs().stream().findAny().orElse(null);

            if (config == null) {
                return;
            }

            if (config.loadBalancerGwMac() != null) {
                log.info("Peer Mac {} for KubernetesExternalLbGateway is retrieved. Stop this task.",
                        config.loadBalancerGwMac());
                this.cancel();
                return;
            }

            PortNumber externalPatchPortNum = KubevirtNetworkingUtil.externalPatchPortNum(deviceService, gatewayNode);

            if (externalPatchPortNum == null) {
                log.warn("processKubernetesExternalLbConfigMacLearning" +
                                " called but there's no external patchPort for {}. Stop this task.",
                        gatewayNode);
                return;
            }

            retrievePeerMac(srcIp, srcMac, peerIp, gatewayNode, externalPatchPortNum);
        }
    }


    private class KubernetexExternalLbIntfTimerTask extends TimerTask {
        private final IpAddress srcIp;
        private final MacAddress srcMac;
        private final IpAddress peerIp;
        private final KubevirtNode gatewayNode;

        public KubernetexExternalLbIntfTimerTask(IpAddress srcIp, MacAddress srcMac,
                                                 IpAddress peerIp, KubevirtNode gatewayNode) {
            this.srcIp = srcIp;
            this.srcMac = srcMac;
            this.peerIp = peerIp;
            this.gatewayNode = gatewayNode;
        }

        @Override
        public void run() {

            KubernetesExternalLbInterface externalLbInterface = gatewayNode.kubernetesExternalLbInterface();

            if (externalLbInterface.externalLbGwMac() != null) {
                log.info("Peer Mac {} for KubernetesExternalLbIntfGw for node {} is retrieved. Stop this task.",
                        externalLbInterface.externalLbGwMac(), gatewayNode.hostname());
                this.cancel();
                return;
            }

            PortNumber elbPatchPortNum = KubevirtNetworkingUtil.elbPatchPortNum(deviceService, gatewayNode);

            if (elbPatchPortNum == null) {
                log.warn("processKubernetesExternalLbIntfGwMacLearningForGatewayNode" +
                                " called but there's no elb patchPort for {}. Stop this task.",
                        gatewayNode);
                return;
            }

            retrievePeerMac(srcIp, srcMac, peerIp, gatewayNode, elbPatchPortNum);
        }
    }

    private class InternalPacketProcessor implements PacketProcessor {
        @Override
        public void process(PacketContext context) {
            if (context.isHandled()) {
                return;
            }

            InboundPacket pkt = context.inPacket();
            Ethernet ethernet = pkt.parsed();

            if (ethernet != null && ethernet.getEtherType() == Ethernet.TYPE_ARP) {
                processArpPacket(ethernet);
            }
        }

        private void processArpPacket(Ethernet ethernet) {
            ARP arp = (ARP) ethernet.getPayload();

            if (arp.getOpCode() == ARP.OP_REQUEST) {
                return;
            }
            log.trace("ARP request {}", arp);

            KubernetesExternalLbConfig config =
                    externalLbConfigAdminService.lbConfigs().stream().findAny().orElse(null);

            IpAddress spa = Ip4Address.valueOf(arp.getSenderProtocolAddress());
            MacAddress sha = MacAddress.valueOf(arp.getSenderHardwareAddress());

            if (config != null && config.loadBalancerGwIp().equals(spa)) {
                externalLbConfigAdminService.updateKubernetesExternalLbConfig(config.updateLbGatewayMac(sha));
            }

            nodeAdminService.completeExternalLbGatewayNodes().forEach(gateway -> {
                KubernetesExternalLbInterface externalLbInterface =
                        gateway.kubernetesExternalLbInterface();
                if (externalLbInterface == null) {
                    return;
                }

                if (externalLbInterface.externalLbGwIp().equals(spa)) {
                    if (externalLbInterface.externalLbGwMac() == null ||
                            !externalLbInterface.externalLbGwMac().equals(sha)) {
                        nodeAdminService.updateNode(gateway.updateKubernetesElbIntfGwMac(sha));
                    }
                }
            });
        }
    }
}

