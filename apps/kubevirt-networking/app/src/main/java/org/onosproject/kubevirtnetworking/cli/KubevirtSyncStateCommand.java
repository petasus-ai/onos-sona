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
package org.onosproject.kubevirtnetworking.cli;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.kubevirtnetworking.api.KubevirtLoadBalancer;
import org.onosproject.kubevirtnetworking.api.KubevirtLoadBalancerAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtPeerRouter;
import org.onosproject.kubevirtnetworking.api.KubevirtRouter;
import org.onosproject.kubevirtnetworking.api.KubevirtRouterAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroup;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroupAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroupRule;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeAdminService;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.buildKubevirtNode;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseKubevirtNetwork;

/**
 * Synchronizes kubevirt states.
 */
@Service
@Command(scope = "onos", name = "kubevirt-sync-state",
        description = "Synchronizes kubevirt states.")
public class KubevirtSyncStateCommand extends AbstractShellCommand {

    private static final String ITEMS = "items";
    private static final String SPEC = "spec";
    private final CustomResourceDefinitionContext routerCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("virtualrouters")
            .build();

    private final CustomResourceDefinitionContext nadCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("k8s.cni.cncf.io")
            .withScope("Namespaced")
            .withVersion("v1")
            .withPlural("network-attachment-definitions")
            .build();

    private final CustomResourceDefinitionContext securityGroupCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("securitygroups")
            .build();

    private final CustomResourceDefinitionContext securityGroupRuleCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("securitygrouprules")
            .build();

    private final CustomResourceDefinitionContext lbCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("loadbalancers")
            .build();

    @Override
    protected void doExecute() throws Exception {
        KubevirtApiConfigService apiConfigService = get(KubevirtApiConfigService.class);

        print("Re-synchronizing Kubevirt states..");
        KubevirtApiConfig config = apiConfigService.apiConfig();

        // the client owns an OkHttp dispatcher and connection pool, so close
        // it once the sync is done or every CLI invocation leaks its threads
        try (KubernetesClient k8sClient = k8sClient(config)) {
            if (k8sClient == null) {
                error("Failed to initialize Kubernetes client.");
                return;
            }

            // try to sync nodes
            syncNodes(k8sClient);

            // try to sync networks
            syncNetworks(k8sClient);

            // try to sync routers
            syncRouters(k8sClient);

            // try to sync security groups
            syncSecurityGroups(k8sClient);

            // try to sync security group rules
            syncSecurityGroupRules(k8sClient);

            // try to sync load balancers
            syncLoadBalancers(k8sClient);
        }

        print("Done.");
    }

    private void syncNodes(KubernetesClient client) {
        KubevirtNodeAdminService nodeService = get(KubevirtNodeAdminService.class);
        Set<KubevirtNode> existingNodes = nodeService.nodes();
        Set<String> existingNodeNames = existingNodes.stream()
                .map(KubevirtNode::hostname).collect(Collectors.toSet());
        List<Node> refNodes = client.nodes().list().getItems();

        for (Node node : refNodes) {
            String nodeName = node.getMetadata().getName();
            KubevirtNode builtNode = buildKubevirtNode(node);
            if (existingNodeNames.contains(nodeName)) {
                nodeService.updateNode(builtNode);
            } else {
                nodeService.createNode(builtNode);
            }
        }

        print("Successfully synchronized nodes!");
    }

    private void syncRouters(KubernetesClient client) {
        KubevirtRouterAdminService routerService = get(KubevirtRouterAdminService.class);
        Set<KubevirtRouter> existingRouters = routerService.routers();
        Set<String> existingRouterNames = existingRouters.stream()
                .map(KubevirtRouter::id).collect(Collectors.toSet());
        Map<String, Object> refRouters = client.customResource(routerCrdCxt).list();
        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(refRouters);
            JsonObject json = JsonObject.readFrom(jsonString);
            JsonArray items = json.get(ITEMS).asArray();

            for (JsonValue item : items) {
                KubevirtRouter router = parseKubevirtRouter(routerService, item.toString());
                if (router != null) {
                    if (existingRouterNames.contains(router.id())) {

                        KubevirtPeerRouter oldPeerRouter = routerService.router(router.id()).peerRouter();
                        if (oldPeerRouter != null
                                && Objects.equals(oldPeerRouter.ipAddress(), router.peerRouter().ipAddress())
                                && oldPeerRouter.macAddress() != null
                                && router.peerRouter().macAddress() == null) {

                            router = router.updatePeerRouter(oldPeerRouter);
                        }

                        routerService.updateRouter(router);
                    } else {
                        routerService.createRouter(router);
                    }
                }
            }
        } catch (Exception e) {
            error("Failed to synchronize routers! Reason: " + e.getMessage());
            return;
        }

        print("Successfully synchronized routers!");
    }

    private void syncNetworks(KubernetesClient client) {
        KubevirtNetworkAdminService networkService = get(KubevirtNetworkAdminService.class);
        Set<KubevirtNetwork> existingNetworks = networkService.networks();
        Set<String> existingNetworkNames = existingNetworks.stream()
                .map(KubevirtNetwork::name).collect(Collectors.toSet());
        Map<String, Object> refNetworks = client.customResource(nadCrdCxt).list();
        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(refNetworks);
            JsonObject json = JsonObject.readFrom(jsonString);
            JsonArray items = json.get(ITEMS).asArray();
            for (JsonValue item : items) {
                KubevirtNetwork network = parseKubevirtNetwork(item.toString());
                if (network != null) {
                    if (existingNetworkNames.contains(network.name())) {
                        networkService.updateNetwork(network);
                    } else {
                        networkService.createNetwork(network);
                    }
                }
            }
        } catch (Exception e) {
            error("Failed to synchronize networks! Reason: " + e.getMessage());
            return;
        }

        print("Successfully synchronized networks!");
    }

    private void syncSecurityGroups(KubernetesClient client) {
        KubevirtSecurityGroupAdminService sgService = get(KubevirtSecurityGroupAdminService.class);
        Set<KubevirtSecurityGroup> existingSgs = sgService.securityGroups();
        Set<String> existingSgNames = existingSgs.stream()
                .map(KubevirtSecurityGroup::id).collect(Collectors.toSet());
        Map<String, Object> refSgs = client.customResource(securityGroupCrdCxt).list();
        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(refSgs);
            JsonObject json = JsonObject.readFrom(jsonString);
            JsonArray items = json.get(ITEMS).asArray();
            for (JsonValue item : items) {
                KubevirtSecurityGroup sg = parseSecurityGroup(item.toString());
                if (sg != null) {
                    if (existingSgNames.contains(sg.id())) {
                        KubevirtSecurityGroup orig = sgService.securityGroup(sg.id());

                        if (orig != null) {
                            KubevirtSecurityGroup updated = sg.updateRules(orig.rules());
                            sgService.updateSecurityGroup(updated);
                        }
                    } else {
                        sgService.createSecurityGroup(sg);
                    }
                }
            }
        } catch (Exception e) {
            error("Failed to synchronize security groups! Reason: " + e.getMessage());
            return;
        }

        print("Successfully synchronized security groups!");
    }

    private void syncSecurityGroupRules(KubernetesClient client) {
        KubevirtSecurityGroupAdminService sgService = get(KubevirtSecurityGroupAdminService.class);
        Set<KubevirtSecurityGroup> existingSgs = sgService.securityGroups();
        Set<KubevirtSecurityGroupRule> existingSgrs = new HashSet<>();
        existingSgs.forEach(sg -> existingSgrs.addAll(sg.rules()));
        Set<String> existingSgrIds = existingSgrs.stream()
                .map(KubevirtSecurityGroupRule::id).collect(Collectors.toSet());
        Map<String, Object> refSgrs = client.customResource(securityGroupRuleCrdCxt).list();
        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(refSgrs);
            JsonObject json = JsonObject.readFrom(jsonString);
            JsonArray items = json.get(ITEMS).asArray();

            for (JsonValue item : items) {
                KubevirtSecurityGroupRule sgr = parseSecurityGroupRule(item.toString());
                if (sgr != null) {
                    if (!existingSgrIds.contains(sgr.id())) {
                        sgService.createSecurityGroupRule(sgr);
                    }
                }
            }
        } catch (Exception e) {
            error("Failed to synchronize security group rules! Reason: " + e.getMessage());
            return;
        }

        print("Successfully synchronized security group rules!");
    }

    private void syncLoadBalancers(KubernetesClient client) {
        KubevirtLoadBalancerAdminService lbService = get(KubevirtLoadBalancerAdminService.class);
        Set<KubevirtLoadBalancer> existingLbs = lbService.loadBalancers();
        Set<String> existingLbNames = existingLbs.stream()
                .map(KubevirtLoadBalancer::id).collect(Collectors.toSet());
        Map<String, Object> refLbs = client.customResource(lbCrdCxt).list();
        ObjectMapper mapper = new ObjectMapper();

        try {
            String jsonString = mapper.writeValueAsString(refLbs);
            JsonObject json = JsonObject.readFrom(jsonString);
            JsonArray items = json.get(ITEMS).asArray();

            for (JsonValue item : items) {
                KubevirtLoadBalancer lb = parseKubevirtLoadBalancer(item.toString());
                if (lb != null) {
                    if (existingLbNames.contains(lb.id())) {
                        lbService.updateLoadBalancer(lb);
                    } else {
                        lbService.createLoadBalancer(lb);
                    }
                }
            }

        } catch (Exception e) {
            error("Failed to synchronize load balancers! Reason: " + e.getMessage());
            return;
        }

        print("Successfully synchronized load balancers!");
    }

    private KubevirtRouter parseKubevirtRouter(KubevirtRouterAdminService service, String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            ObjectNode spec = (ObjectNode) json.get(SPEC);

            KubevirtRouter router = codec(KubevirtRouter.class).decode(spec, this);
            KubevirtRouter existing = service.router(router.id());

            if (existing == null) {
                return router;
            } else {
                return router.updatedElectedGateway(existing.electedGateway());
            }
        } catch (IOException e) {
            log.error("Failed to parse kubevirt router object");
        }

        return null;
    }

    private KubevirtSecurityGroup parseSecurityGroup(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            ObjectNode spec = (ObjectNode) json.get(SPEC);
            return codec(KubevirtSecurityGroup.class).decode(spec, this);
        } catch (IOException e) {
            log.error("Failed to parse kubevirt security group object");
        }

        return null;
    }

    private KubevirtSecurityGroupRule parseSecurityGroupRule(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            ObjectNode spec = (ObjectNode) json.get(SPEC);
            return codec(KubevirtSecurityGroupRule.class).decode(spec, this);
        } catch (IOException e) {
            log.error("Failed to parse kubevirt security group rule object");
        }

        return null;
    }

    private KubevirtLoadBalancer parseKubevirtLoadBalancer(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            ObjectNode spec = (ObjectNode) json.get(SPEC);
            return codec(KubevirtLoadBalancer.class).decode(spec, this);
        } catch (IOException e) {
            log.error("Failed to parse kubevirt load balancer object");
        }

        return null;
    }
}
