/*
 * Copyright 2025-present Open Networking Foundation
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

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IpAddress;
import org.onlab.packet.IpPrefix;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtFlowRuleService;
import org.onosproject.kubevirtnetworking.api.KubevirtIpPool;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkEvent;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkListener;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeEvent;
import org.onosproject.kubevirtnode.api.KubevirtNodeListener;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
import org.onosproject.net.DeviceId;
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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.COMMON_ACL_EGRESS_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.COMMON_ACL_INGRESS_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.COMMON_ACL_RECIRC_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.COMMON_FORWARDING_TABLE;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.api.Constants.PRIORITY_KAAS_ELB_RULE;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.waitFor;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Handles KaaS External load balancer.
 */
@Component(immediate = true)
public class KaasExternalLbHandler {
    protected final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LeadershipService leadershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeService nodeService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNetworkService networkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtFlowRuleService flowService;

    private final KubevirtNodeListener nodeListener = new InternalNodeListener();

    private final KubevirtNetworkListener networkListener = new InternalNetworkListener();

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler"));

    private ApplicationId appId;
    private NodeId localNodeId;

    // Tracks the LB IP pool last programmed per network (keyed by networkId) so
    // that a pool change (Service Hub PUT/DELETE -> NAD MODIFIED) can uninstall
    // the previous range's admit rules before installing the new one.
    private final Map<String, KubevirtIpPool> lbPoolByNetwork = Maps.newConcurrentMap();

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(KUBEVIRT_NETWORKING_APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());
        nodeService.addListener(nodeListener);
        networkService.addListener(networkListener);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        leadershipService.withdraw(appId.name());
        nodeService.removeListener(nodeListener);
        networkService.removeListener(networkListener);

        eventExecutor.shutdown();

        log.info("Stopped");
    }

    /**
     * Programs (or removes) the SG-bypass admit rules for a single LB IP on one
     * physnet bridge. Mirrors the per-VM fixed-IP path but keys on the LB IP only
     * (no dl_dst/dl_src MAC match) so it holds for whichever node MetalLB elects as
     * the L2 leader. Priority is above the per-port SG rules and the conntrack
     * catch-all so off-subnet traffic to the LB IP reaches forwarding instead of
     * the default-deny.
     *
     * @param physBridge physnet bridge device
     * @param lbIp       load balancer IP
     * @param install    true to install, false to remove
     */
    private void applyLbIpBridgeRules(DeviceId physBridge, IpAddress lbIp, boolean install) {
        if (physBridge == null || lbIp == null) {
            return;
        }

        IpPrefix lbPrefix = lbIp.toIpPrefix();

        // EGRESS (40): LB IP -> external reply, forward to recirc (43).
        flowService.setRule(appId, physBridge,
                DefaultTrafficSelector.builder()
                        .matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPSrc(lbPrefix).build(),
                DefaultTrafficTreatment.builder()
                        .transition(COMMON_ACL_RECIRC_TABLE).build(),
                PRIORITY_KAAS_ELB_RULE, COMMON_ACL_EGRESS_TABLE, install);

        // RECIRC (43): external -> LB IP, divert to ingress ACL (44) ahead of the
        // lower-priority conntrack catch-all so the next table is the admit table.
        flowService.setRule(appId, physBridge,
                DefaultTrafficSelector.builder()
                        .matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPDst(lbPrefix).build(),
                DefaultTrafficTreatment.builder()
                        .transition(COMMON_ACL_INGRESS_TABLE).build(),
                PRIORITY_KAAS_ELB_RULE, COMMON_ACL_RECIRC_TABLE, install);

        // INGRESS (44): external -> LB IP, admit to forwarding (80).
        flowService.setRule(appId, physBridge,
                DefaultTrafficSelector.builder()
                        .matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPDst(lbPrefix).build(),
                DefaultTrafficTreatment.builder()
                        .transition(COMMON_FORWARDING_TABLE).build(),
                PRIORITY_KAAS_ELB_RULE, COMMON_ACL_INGRESS_TABLE, install);
    }

    private class InternalNetworkListener implements KubevirtNetworkListener {

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtNetworkEvent event) {
            switch (event.type()) {
                case KUBEVIRT_NETWORK_CREATED:
                    eventExecutor.execute(() -> processNetworkCreation(event.subject()));
                    break;
                case KUBEVIRT_NETWORK_REMOVED:
                    eventExecutor.execute(() -> processNetworkRemoval(event.subject()));
                    break;
                case KUBEVIRT_NETWORK_UPDATED:
                    eventExecutor.execute(() -> processNetworkUpdate(event.subject()));
                    break;
                default:
                    // do nothing
                    break;
            }
        }

        private void processNetworkCreation(KubevirtNetwork network) {
            if (!isRelevantHelper()) {
                return;
            }

            switch (network.type()) {
                case FLAT:
                    setElbIngressRules(network, true);
                    setElbEgressRules(network, true);
                    setElbLbPoolRules(network, network.lbIpPool(), true);
                    break;
                case VXLAN:
                case GRE:
                case GENEVE:
                case VLAN:
                default:
                    // do nothing
                    break;
            }
        }

        private void processNetworkRemoval(KubevirtNetwork network) {
            if (!isRelevantHelper()) {
                return;
            }

            switch (network.type()) {
                case FLAT:
                    setElbIngressRules(network, false);
                    setElbEgressRules(network, false);
                    setElbLbPoolRules(network, network.lbIpPool(), false);
                    lbPoolByNetwork.remove(network.networkId());
                    break;
                case VXLAN:
                case GRE:
                case GENEVE:
                case VLAN:
                default:
                    // do nothing
                    break;
            }
        }

        private void processNetworkUpdate(KubevirtNetwork network) {
            if (!isRelevantHelper()) {
                return;
            }

            if (network.type() != KubevirtNetwork.Type.FLAT) {
                return;
            }

            KubevirtIpPool prev = lbPoolByNetwork.get(network.networkId());
            KubevirtIpPool now = network.lbIpPool();

            if (Objects.equals(prev, now)) {
                return;
            }

            // The LB IP pool changed (Service Hub PUT/DELETE patched the NAD).
            // Remove the previous range's admit rules first, then install the new
            // range (or just clean up the tracking entry when it was cleared).
            if (prev != null) {
                setElbLbPoolRules(network, prev, false);
            }

            if (now != null) {
                setElbLbPoolRules(network, now, true);
            } else {
                lbPoolByNetwork.remove(network.networkId());
            }
        }

        // Installs (install=true) or removes the SG-bypass "allow" rules for every
        // IP in the network's LB IP pool on each worker's physnet bridge. This
        // mirrors the per-VM fixed-IP admit path (tables 40/43/44 -> forwarding)
        // but keys purely on the LB IP, dropping the dl_dst/dl_src MAC match so the
        // rule works no matter which node MetalLB elects as the L2 leader for the
        // IP. Without it, off-subnet traffic to an LB IP hits the SG default-deny
        // (the LB IP is no VM port's fixed IP); intra-subnet already works via the
        // existing FLAT CIDR rule, these add the off-subnet path.
        private void setElbLbPoolRules(KubevirtNetwork network, KubevirtIpPool lbPool, boolean install) {
            if (network.type() != KubevirtNetwork.Type.FLAT || lbPool == null) {
                return;
            }

            nodeService.completeNodes(WORKER).forEach(n -> {
                Set<KubevirtPhyInterface> kpis = n.phyIntfs().stream().filter(pi ->
                        StringUtils.equals(pi.network(), network.physnetName()))
                        .collect(Collectors.toSet());

                kpis.forEach(kpi -> lbPool.availableIps().forEach(lbIp ->
                        applyLbIpBridgeRules(kpi.physBridge(), lbIp, install)));
            });

            if (install) {
                lbPoolByNetwork.put(network.networkId(), lbPool);
            }
        }

        private void setElbIngressRules(KubevirtNetwork network, boolean install) {
            nodeService.completeNodes(WORKER).forEach(n -> {
                Set<KubevirtPhyInterface> kpis = n.phyIntfs().stream().filter(pi ->
                        StringUtils.equals(pi.network(), network.physnetName())).collect(Collectors.toSet());
                kpis.forEach(kpi -> kpi.kaasElbs().forEach(ke -> {
                    TrafficSelector selector = DefaultTrafficSelector.builder()
                            .matchEthType(Ethernet.TYPE_IPV4)
                            .matchIPSrc(IpPrefix.valueOf(ke))
                            .matchIPDst(IpPrefix.valueOf(network.cidr()))
                            .build();

                    TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                            .transition(COMMON_FORWARDING_TABLE)
                            .build();

                    // Rule for intra-subnet IPs
                    flowService.setRule(
                            appId,
                            kpi.physBridge(),
                            selector,
                            treatment,
                            PRIORITY_KAAS_ELB_RULE,
                            COMMON_ACL_RECIRC_TABLE,
                            install
                    );

                    // Rule for external IPs
                    flowService.setRule(
                            appId,
                            kpi.physBridge(),
                            selector,
                            treatment,
                            PRIORITY_KAAS_ELB_RULE,
                            COMMON_ACL_INGRESS_TABLE,
                            install
                    );
                }));
            });
        }

        private void setElbEgressRules(KubevirtNetwork network, boolean install) {
            nodeService.completeNodes(WORKER).forEach(n -> {
                Set<KubevirtPhyInterface> kpis = n.phyIntfs().stream().filter(pi ->
                        StringUtils.equals(pi.network(), network.physnetName())).collect(Collectors.toSet());
                kpis.forEach(kpi -> kpi.kaasElbs().forEach(ke -> {
                    TrafficSelector selector = DefaultTrafficSelector.builder()
                            .matchEthType(Ethernet.TYPE_IPV4)
                            .matchIPSrc(IpPrefix.valueOf(network.cidr()))
                            .matchIPDst(IpPrefix.valueOf(ke))
                            .build();

                    TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                            .transition(COMMON_FORWARDING_TABLE)
                            .build();

                    flowService.setRule(
                            appId,
                            kpi.physBridge(),
                            selector,
                            treatment,
                            PRIORITY_KAAS_ELB_RULE,
                            COMMON_ACL_EGRESS_TABLE,
                            install
                    );
                }));
            });
        }
    }

    private class InternalNodeListener implements KubevirtNodeListener {

        @Override
        public boolean isRelevant(KubevirtNodeEvent event) {
            return event.subject().type() == WORKER;
        }

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtNodeEvent event) {
            switch (event.type()) {
                case KUBEVIRT_NODE_COMPLETE:
                    eventExecutor.execute(() -> processNodeComplete(event.subject()));
                    break;
                default:
                    break;
            }
        }

        private void processNodeComplete(KubevirtNode node) {
            if (!isRelevantHelper()) {
                return;
            }

            // FIXME: we wait all port get its deviceId updated
            waitFor(5);

            // setElbIngressRules(node, true);
            // setElbEgressRules(node, true);

            setElbFlatRules(node, true);

            // A worker that joins after the LB-enabled network was created would
            // otherwise miss the pool admit rules (the network event only iterated
            // the workers present at that time), so (re)apply them on this node.
            setElbLbPoolRules(node, true);
        }

        private void setElbIngressRules(KubevirtNode node, boolean install) {

            node.phyIntfs().forEach(pi -> {
                if (pi.physBridge() != null && pi.network() != null) {
                    Set<KubevirtNetwork> kns = networkService.networks().stream().filter(n ->
                            StringUtils.equals(pi.network(), n.physnetName())).collect(Collectors.toSet());
                    kns.forEach(kn -> pi.kaasElbs().forEach(ke -> {
                        TrafficSelector selector = DefaultTrafficSelector.builder()
                                .matchEthType(Ethernet.TYPE_IPV4)
                                .matchIPSrc(IpPrefix.valueOf(ke))
                                .matchIPDst(IpPrefix.valueOf(kn.cidr()))
                                .build();

                        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                                .transition(COMMON_FORWARDING_TABLE)
                                .build();

                        // Rule for intra-subnet IPs
                        flowService.setRule(
                                appId,
                                pi.physBridge(),
                                selector,
                                treatment,
                                PRIORITY_KAAS_ELB_RULE,
                                COMMON_ACL_RECIRC_TABLE,
                                install
                        );

                        // Rule for external IPs
                        flowService.setRule(
                                appId,
                                pi.physBridge(),
                                selector,
                                treatment,
                                PRIORITY_KAAS_ELB_RULE,
                                COMMON_ACL_INGRESS_TABLE,
                                install
                        );
                    }));
                }
            });
        }

        private void setElbEgressRules(KubevirtNode node, boolean install) {
            node.phyIntfs().forEach(pi -> {
                if (pi.physBridge() != null && pi.network() != null) {
                    Set<KubevirtNetwork> kns = networkService.networks().stream().filter(n ->
                            StringUtils.equals(pi.network(), n.physnetName())).collect(Collectors.toSet());
                    kns.forEach(kn -> pi.kaasElbs().forEach(ke -> {
                        TrafficSelector selector = DefaultTrafficSelector.builder()
                                .matchEthType(Ethernet.TYPE_IPV4)
                                .matchIPSrc(IpPrefix.valueOf(kn.cidr()))
                                .matchIPDst(IpPrefix.valueOf(ke))
                                .build();

                        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                                .transition(COMMON_FORWARDING_TABLE)
                                .build();

                        flowService.setRule(
                                appId,
                                pi.physBridge(),
                                selector,
                                treatment,
                                PRIORITY_KAAS_ELB_RULE,
                                COMMON_ACL_EGRESS_TABLE,
                                install
                        );
                    }));
                }
            });
        }

        private void setElbFlatRules(KubevirtNode node, boolean install) {
            node.phyIntfs().forEach(pi -> {
                if (pi.physBridge() != null && pi.network() != null) {
                    Set<KubevirtNetwork> kns = networkService.networks().stream().filter(n ->
                            StringUtils.equals(pi.network(), n.physnetName()) &&
                                    n.type() == KubevirtNetwork.Type.FLAT).collect(Collectors.toSet());
                    kns.forEach(kn -> {
                        TrafficSelector selector = DefaultTrafficSelector.builder()
                                .matchEthType(Ethernet.TYPE_IPV4)
                                .matchIPSrc(IpPrefix.valueOf(kn.cidr()))
                                .matchIPDst(IpPrefix.valueOf(kn.cidr()))
                                .build();

                        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                                .transition(COMMON_FORWARDING_TABLE)
                                .build();

                        flowService.setRule(
                                appId,
                                pi.physBridge(),
                                selector,
                                treatment,
                                PRIORITY_KAAS_ELB_RULE,
                                COMMON_ACL_EGRESS_TABLE,
                                install
                        );

                        flowService.setRule(
                                appId,
                                pi.physBridge(),
                                selector,
                                treatment,
                                PRIORITY_KAAS_ELB_RULE,
                                COMMON_ACL_INGRESS_TABLE,
                                install
                        );
                    });
                }
            });
        }

        // Node-scoped LB pool installer (mirror of setElbFlatRules(node)): applied
        // on KUBEVIRT_NODE_COMPLETE so a newly joined worker also gets the admit
        // rules for every FLAT network that has an LB IP pool configured.
        private void setElbLbPoolRules(KubevirtNode node, boolean install) {
            node.phyIntfs().forEach(pi -> {
                if (pi.physBridge() != null && pi.network() != null) {
                    networkService.networks().stream()
                            .filter(kn -> kn.type() == KubevirtNetwork.Type.FLAT &&
                                    StringUtils.equals(pi.network(), kn.physnetName()) &&
                                    kn.lbIpPool() != null)
                            .forEach(kn -> kn.lbIpPool().availableIps().forEach(lbIp ->
                                    applyLbIpBridgeRules(pi.physBridge(), lbIp, install)));
                }
            });
        }
    }
}
