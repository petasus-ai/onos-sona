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

import org.apache.commons.lang.StringUtils;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IpPrefix;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtFlowRuleService;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkEvent;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkListener;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeEvent;
import org.onosproject.kubevirtnode.api.KubevirtNodeListener;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
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
                    break;
                case VXLAN:
                case GRE:
                case GENEVE:
                case STT:
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
                    break;
                case VXLAN:
                case GRE:
                case GENEVE:
                case STT:
                case VLAN:
                default:
                    // do nothing
                    break;
            }
        }

        private void setElbIngressRules(KubevirtNetwork network, boolean install) {
            nodeService.completeNodes(WORKER).forEach(n -> {
                Set<KubevirtPhyInterface> kpis = n.phyIntfs().stream().filter(pi ->
                        StringUtils.equals(pi.network(), network.physnetName())).collect(Collectors.toSet());

                // we derive the ELB networks which are managed under the same project/namespace
                Set<KubevirtNetwork> ens = networkService.networks().stream()
                        .filter(KubevirtNetwork::isElbDedicated)
                        .filter(elbnet -> elbnet.project().equals(network.project()))
                        .collect(Collectors.toSet());

                kpis.forEach(kpi -> ens.forEach(en -> {
                            TrafficSelector selector = DefaultTrafficSelector.builder()
                                    .matchEthType(Ethernet.TYPE_IPV4)
                                    .matchIPSrc(IpPrefix.valueOf(en.cidr()))
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
                        }
                ));
            });
        }

        private void setElbEgressRules(KubevirtNetwork network, boolean install) {
            nodeService.completeNodes(WORKER).forEach(n -> {
                Set<KubevirtPhyInterface> kpis = n.phyIntfs().stream().filter(pi ->
                        StringUtils.equals(pi.network(), network.physnetName())).collect(Collectors.toSet());

                // we derive the ELB networks which are managed under the same project/namespace
                Set<KubevirtNetwork> ens = networkService.networks().stream()
                        .filter(KubevirtNetwork::isElbDedicated)
                        .filter(elbnet -> elbnet.project().equals(network.project()))
                        .collect(Collectors.toSet());

                kpis.forEach(kpi -> ens.forEach(en -> {
                    TrafficSelector selector = DefaultTrafficSelector.builder()
                            .matchEthType(Ethernet.TYPE_IPV4)
                            .matchIPSrc(IpPrefix.valueOf(network.cidr()))
                            .matchIPDst(IpPrefix.valueOf(en.cidr()))
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

            setElbFlatRules(node, true);
        }

        private void setElbFlatRules(KubevirtNode node, boolean install) {
            node.phyIntfs().forEach(pi -> {
                if (pi.physBridge() != null && pi.network() != null) {
                    Set<KubevirtNetwork> kns = networkService.networks().stream()
                            .filter(n -> StringUtils.equals(pi.network(), n.physnetName()))
                            .filter(n -> n.type() == KubevirtNetwork.Type.FLAT)
                            .filter(n -> !n.isElbDedicated())
                            .collect(Collectors.toSet());
                    kns.forEach(kn -> networkService.networks().stream()
                            .filter(KubevirtNetwork::isElbDedicated)
                            .filter(n -> n.project().equals(kn.project()))
                            .forEach(en -> {
                                TrafficSelector ingressSelector = DefaultTrafficSelector.builder()
                                        .matchEthType(Ethernet.TYPE_IPV4)
                                        .matchIPSrc(IpPrefix.valueOf(en.cidr()))
                                        .matchIPDst(IpPrefix.valueOf(kn.cidr()))
                                        .build();

                                TrafficSelector egressSelector = DefaultTrafficSelector.builder()
                                        .matchEthType(Ethernet.TYPE_IPV4)
                                        .matchIPSrc(IpPrefix.valueOf(kn.cidr()))
                                        .matchIPDst(IpPrefix.valueOf(en.cidr()))
                                        .build();

                                TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                                        .transition(COMMON_FORWARDING_TABLE)
                                        .build();

                                // Rule for egress
                                flowService.setRule(
                                        appId,
                                        pi.physBridge(),
                                        egressSelector,
                                        treatment,
                                        PRIORITY_KAAS_ELB_RULE,
                                        COMMON_ACL_EGRESS_TABLE,
                                        install
                                );

                                // Rule for ingress, handle intra-subnet
                                flowService.setRule(
                                        appId,
                                        pi.physBridge(),
                                        ingressSelector,
                                        treatment,
                                        PRIORITY_KAAS_ELB_RULE,
                                        COMMON_ACL_RECIRC_TABLE,
                                        install
                                );

                                // Rule for ingress, handle external IPs
                                flowService.setRule(
                                        appId,
                                        pi.physBridge(),
                                        ingressSelector,
                                        treatment,
                                        PRIORITY_KAAS_ELB_RULE,
                                        COMMON_ACL_INGRESS_TABLE,
                                        install
                                );
                    }));
                }
            });
        }
    }
}
