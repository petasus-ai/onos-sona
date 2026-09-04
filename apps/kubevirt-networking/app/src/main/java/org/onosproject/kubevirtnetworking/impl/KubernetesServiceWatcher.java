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


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.DefaultKubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.DefaultKubernetesServicePort;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLbAdminService;
import org.onosproject.kubevirtnetworking.api.KubernetesServicePort;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigListener;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigService;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigEvent;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigListener;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeEvent;
import org.onosproject.kubevirtnode.api.KubevirtNodeListener;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
import org.onosproject.mastership.MastershipService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.configMapUpdated;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.gatewayNodeForSpecifiedService;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.workerNodeForSpecifiedService;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes service watcher used for external loadbalancing among PODs.
 */
@Component(immediate = true)
public class KubernetesServiceWatcher {
    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected MastershipService mastershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LeadershipService leadershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService apiConfigService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubernetesExternalLbConfigService lbConfigService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubernetesExternalLbAdminService adminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeService nodeService;

    private static final String KUBE_DASH_VIP = "kube-vip";
    private static final String KUBE_VIP = "kubevip";
    private static final String LOADBALANCER_IP = "loadBalancerIP";
    private static final String TYPE_LOADBALANCER = "LoadBalancer";
    private static final String KUBE_SYSTEM = "kube-system";
    private static final String GATEWAY_IP = "gateway-ip";
    private static final String GATEWAY_MAC = "gateway-mac";
    private static final String DEFAULT = "default";

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));

    private static final long RECONNECT_DELAY_S = 5;

    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));

    // the store resync (list + prune) makes a blocking API-server call, so it
    // runs off both the event and reconnect threads to avoid stalling event
    // processing or delaying a pending re-watch
    private final ExecutorService resyncExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "store-resync", log));

    // the client owning the currently active watch; closing it terminates the
    // watch, so keeping exactly one instance prevents both duplicated watches
    // and client (thread/connection pool) leaks on re-instantiation
    private volatile KubernetesClient watchClient;

    // the handle of the active watch; it MUST be closed before its client,
    // otherwise the fabric8 watch manager keeps re-running its reconnect
    // loop on top of the client's terminated dispatcher, flooding the log
    // with "Exec Failure ... executor rejected" warnings
    private Watch watch;

    // set while WE close the watch on purpose (re-instantiation, shutdown);
    // an onClose callback fired by that intentional close must not schedule
    // another re-instantiation, or the watch would flap forever
    private volatile boolean closingWatch;

    private final InternalKubevirtApiConfigListener
            apiConfigListener = new InternalKubevirtApiConfigListener();

    private final InternalKubernetesServiceWatcher
            serviceWatcher = new InternalKubernetesServiceWatcher();

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

        apiConfigService.addListener(apiConfigListener);
        lbConfigService.addListener(lbConfigListener);
        nodeService.addListener(nodeEventListener);

        // a restarted instance never sees the API config UPDATED event again,
        // so establish the watch from the current config as well; every
        // instance keeps a watch and the leader check in the event handlers
        // decides who processes the events, which also covers leadership moves
        eventExecutor.execute(this::instantiateWatcher);

        log.info("Started");
    }


    @Deactivate
    protected void deactivate() {
        leadershipService.withdraw(appId.name());

        apiConfigService.removeListener(apiConfigListener);
        lbConfigService.removeListener(lbConfigListener);
        nodeService.removeListener(nodeEventListener);

        reconnectExecutor.shutdown();
        resyncExecutor.shutdown();
        eventExecutor.shutdown();
        closeWatch();
        closeWatchClient();

        log.info("Stopped");
    }

    private synchronized void instantiateWatcher() {
        closeWatch();
        closeWatchClient();
        KubernetesClient client = k8sClient(apiConfigService);
        watchClient = client;

        if (client == null) {
            scheduleReconnect();
            return;
        }

        try {
            watch = client.services().inAnyNamespace().watch(serviceWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
            return;
        }

        // the watch replay upserts current services (addOrUpdateExternalLoadBalancer),
        // but the API server never reports services deleted while the watch was
        // down, so reconcile the external LB store against a fresh listing to
        // prune the strays
        resyncExecutor.execute(() -> resyncStore(client));
    }

    private synchronized void closeWatch() {
        if (watch != null) {
            try {
                closingWatch = true;
                watch.close();
            } catch (Exception e) {
                log.debug("Failed to close the previous watch", e);
            } finally {
                closingWatch = false;
            }
            watch = null;
        }
    }

    private synchronized void closeWatchClient() {
        if (watchClient != null) {
            watchClient.close();
            watchClient = null;
        }
    }

    private void scheduleReconnect() {
        reconnectExecutor.schedule(this::instantiateWatcher,
                RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    private boolean isLeader() {
        return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
    }

    /**
     * Reconciles the external LB store against the API server, removing entries
     * whose service was deleted (or is no longer an eligible load balancer)
     * while this watch was down. The store holds an entry per default-namespace
     * LoadBalancer service carrying the kube-vip label, so the live set applies
     * the same predicates. Runs only on the leader; a failed listing skips the
     * prune rather than delete live state.
     *
     * @param client the client whose watch triggered this resync
     */
    private void resyncStore(KubernetesClient client) {
        if (!isLeader()) {
            return;
        }

        // a newer (re)connect may have already superseded and closed this
        // client; listing on a closed client just throws "Canceled". The
        // replacement connection queues its own resync, so skip rather than
        // log a spurious failure.
        if (client != watchClient) {
            return;
        }

        // snapshot the store keys before listing: an entry whose service ADDED
        // is still in flight must not be mistaken for a stray
        Set<String> storedNames = adminService.loadBalancers().stream()
                .map(KubernetesExternalLb::serviceName)
                .collect(Collectors.toSet());

        Set<String> liveNames;
        try {
            liveNames = client.services().inNamespace(DEFAULT).list().getItems().stream()
                    .filter(this::isLoadBalancerType)
                    .filter(this::isKubeVipCloudProviderLabelIsSet)
                    .map(service -> service.getMetadata().getName())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            // the guard above cannot cover a (re)connect that closes this client
            // while the listing is still in flight (e.g. an API config update
            // right after activation): the request then fails with "Socket
            // closed". The replacement client runs its own resync, so this is
            // not worth a warning.
            if (client != watchClient) {
                log.debug("Skipping external LB resync on a superseded client", e);
                return;
            }
            log.warn("Skipping external LB resync: failed to list services", e);
            return;
        }

        storedNames.stream()
                .filter(name -> !liveNames.contains(name))
                .forEach(name -> {
                    log.info("Pruning stale external LB {} absent from API server", name);
                    try {
                        adminService.removeExternalLb(name);
                    } catch (Exception e) {
                        log.warn("Failed to prune stale external LB {}", name, e);
                    }
                });
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
                    eventExecutor.execute(() -> processConfigUpdate(event.subject()));
                    break;
                case KUBERNETES_EXTERNAL_LB_CONFIG_REMOVED:
                default:
                    //do nothing
                    break;
            }
        }

        private void processConfigUpdate(KubernetesExternalLbConfig externalLbConfig) {
            if (!isRelevantHelper()) {
                return;
            }
            if (configMapUpdated(externalLbConfig)) {
                addOrUpdateExternalLoadBalancers();
            }
        }
    }

    private class InternalKubevirtApiConfigListener implements KubevirtApiConfigListener {

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtApiConfigEvent event) {

            switch (event.type()) {
                case KUBEVIRT_API_CONFIG_UPDATED:
                    eventExecutor.execute(this::processConfigUpdate);
                    break;
                case KUBEVIRT_API_CONFIG_CREATED:
                case KUBEVIRT_API_CONFIG_REMOVED:
                default:
                    // do nothing
                    break;
            }
        }

        private void processConfigUpdate() {
            if (!isRelevantHelper()) {
                return;
            }
            instantiateWatcher();
            addOrUpdateExternalLoadBalancers();
        }
    }

    private class InternalKubernetesServiceWatcher implements Watcher<Service> {

        @Override
        public void eventReceived(Action action, Service service) {
            switch (action) {
                case ADDED:
                case MODIFIED:
                    eventExecutor.execute(() -> processAddOrMod(service));
                    break;
                case DELETED:
                    eventExecutor.execute(() -> processDeletion(service));
                    break;
                case ERROR:
                    log.warn("Failures processing pod manipulation.");
                    break;
                default:
                    break;
            }
        }

        @Override
        public void onClose(WatcherException e) {
            if (closingWatch) {
                // intentional close during re-instantiation or shutdown
                return;
            }
            // the watch dies on API server restarts, resourceVersion expiry
            // (HTTP 410) and fabric8 bugs; re-watch after a short delay so a
            // down API server does not turn this into a tight reconnect loop
            if (e != null && e.isHttpGone()) {
                // expected 410: our (stale) resourceVersion aged out of the
                // API server watch cache; the reconnect re-lists from a fresh
                // version, so log it at debug without the noisy stack trace
                log.debug("Watcher expired (too old resource version), " +
                        "re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleReconnect();
        }

        private void processAddOrMod(Service service) {
            if (service == null || !isMaster() || !isLoadBalancerType(service)) {
                return;
            }

            log.info("Service event ADDED or MODIFIED received");

            KubernetesExternalLbConfig config = lbConfigService.lbConfigs().stream().findAny().orElse(null);

            if (!configMapUpdated(config)) {
                log.warn("Config map is not set yet. Stop this task");
                return;
            }

            try {
                if (addOrUpdateExternalLoadBalancer(service) &&
                        !isLoadBalancerStatusAlreadySet(service)) {
                    serviceStatusUpdate(service);
                }
            } catch (Exception e) {
                log.error("Exception occurred because of {}", e.toString());
            }
        }

        private void processDeletion(Service service) {
            if (service == null || !isMaster() || !isLoadBalancerType(service)) {
                return;
            }

            log.info("Service event DELETED received");

            if (isKubeVipCloudProviderLabelIsSet(service)) {
                KubernetesExternalLb lb = adminService.loadBalancer(service.getMetadata().getName());

                if (lb == null) {
                    return;
                }

                adminService.removeExternalLb(lb.serviceName());
            }
        }
        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }
    }


    //When api config or configmap updated, check every prerequisite and update all external load balancers
    private void addOrUpdateExternalLoadBalancers() {
        try (KubernetesClient client = k8sClient(apiConfigService)) {
            if (client == null) {
                log.warn("Failed to get the kubernetes client, " +
                        "skipping external LB refresh");
                return;
            }

            client.services().inNamespace(DEFAULT).list()
                    .getItems().forEach(service -> {
                        if (addOrUpdateExternalLoadBalancer(service) &&
                                !isLoadBalancerStatusAlreadySet(service)) {
                            serviceStatusUpdate(service);
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to refresh external load balancers", e);
        }
    }

    private boolean addOrUpdateExternalLoadBalancer(Service service) {
        if (isLoadBalancerType(service) &&
                isKubeVipCloudProviderLabelIsSet(service)) {

            KubernetesExternalLb lb = parseKubernetesExternalLb(service);
            if (lb == null) {
                log.warn("Failed to parse the kubernetes external lb");
                return false;
            }

            KubevirtNode electedGatewayNode = gatewayNodeForSpecifiedService(nodeService, lb);
            if (electedGatewayNode == null) {
                log.warn("Service created but there's no gateway nodes ready. Stop this task.");
                return false;
            }

            lb = lb.updateElectedGateway(electedGatewayNode.hostname());

            KubevirtNode electedWorkerNode = workerNodeForSpecifiedService(nodeService, lb);
            if (electedWorkerNode == null) {
                log.warn("Service created but there's no worker nodes ready. Stop this task.");
                return false;
            }
            lb = lb.updateElectedWorker(electedWorkerNode.hostname());

            log.trace("processAddOrMod called and parsed lb is {}", lb);

            KubernetesExternalLb existing = adminService.loadBalancer(lb.serviceName());
            if (existing == null) {
                adminService.createExternalLb(lb);
            } else if (!lb.equals(existing)) {
                // every watch reconnect replays every service as ADDED, so
                // skip the write when nothing changed to avoid churning the
                // store and re-triggering the flow rule handlers
                adminService.updateExternalLb(lb);
            }
            return true;
        }
        return false;
    }

    private void serviceStatusUpdate(Service service) {
        String lbIp = service.getSpec().getLoadBalancerIP();
        if (lbIp == null) {
            return;
        }

        try (KubernetesClient client = k8sClient(apiConfigService)) {
            if (client == null) {
                return;
            }

            // built rather than constructed: the all-args constructor grows
            // with every field Kubernetes adds (ipMode in 1.29)
            LoadBalancerIngress lbIngress = new LoadBalancerIngressBuilder()
                    .withHostname(KUBE_VIP)
                    .withIp(lbIp)
                    .build();

            service.getStatus().getLoadBalancer().setIngress(Lists.newArrayList(lbIngress));

            //When a service is deleted, the event MODIFIED is also along with DELETED event
            //So filter out this MODIFIED events; note that withName() returns a
            //resource handle, so the actual resource must be fetched via get()
            if (client.services().withName(service.getMetadata().getName()).get() != null) {
                client.services().patchStatus(service);
            }
        } catch (Exception e) {
            log.error("Failed to update the status of service {}",
                    service.getMetadata().getName(), e);
        }
    }

    //Only process if the event when the kube-vip-cloud-provider label is set
    // and loadbalancer status is not set.
    private boolean isKubeVipCloudProviderLabelIsSet(Service service) {
        log.trace("isKubeVipCloudProviderLabelIsSet called with labels {}", service.getMetadata().getLabels());
        if (service.getMetadata().getLabels() == null) {
            return false;
        }

        return service.getMetadata().getLabels().containsValue(KUBE_DASH_VIP);
    }

    private boolean isLoadBalancerStatusAlreadySet(Service service) {
        log.trace("isLoadBalancerStatusAlreadySet called with status {}", service.getStatus());

        LoadBalancerStatus lbStatus = service.getStatus().getLoadBalancer();
        if (lbStatus.getIngress().isEmpty()) {
            return false;
        }

        String lbIp = service.getSpec().getLoadBalancerIP();
        if (lbIp == null) {
            return false;
        }

        return lbStatus.getIngress().stream()
                .filter(lbIngress -> Objects.equals(lbIngress.getIp(), lbIp))
                .findAny().isPresent();
    }

    //Only process if the event when the service type is LoadBalancer
    private boolean isLoadBalancerType(Service service) {
        return service.getSpec().getType().equals(TYPE_LOADBALANCER);
    }

    private KubernetesExternalLb parseKubernetesExternalLb(Service service) {
        if (service.getMetadata() == null || service.getSpec() == null) {
            return null;
        }

        String serviceName = service.getMetadata().getName();
        if (serviceName == null) {
            return null;
        }

        String lbIp = service.getSpec().getLoadBalancerIP();
        if (lbIp == null) {
            return null;
        }

        Set<KubernetesServicePort> servicePorts = Sets.newHashSet();
        Set<String> endpointSet = Sets.newHashSet();

        service.getSpec().getPorts().forEach(servicePort -> {
            if (servicePort.getPort() != null && servicePort.getNodePort() != null) {
                servicePorts.add(DefaultKubernetesServicePort.builder()
                        .nodePort(servicePort.getNodePort())
                        .port(servicePort.getPort()).build());
            }
        });

        nodeService.completeNodes(WORKER).forEach(workerNode -> {
            endpointSet.add(workerNode.dataIp().toString());
        });

        IpAddress loadbalancerGatewayIp = loadBalancerGatewayIp();

        if (loadbalancerGatewayIp == null) {
            log.error("Can't find the loadbalancer gateway ip in the kubevip configmap.." +
                    "Failed to parse kubernetes external lb and return null");
            return null;
        }

        MacAddress loadBalancerGatewayMac = loadBalancerGatewayMac();

        if (loadbalancerGatewayIp == null) {
            log.error("Can't find the loadbalancer gateway mac in the kubevip configmap.." +
                    "Failed to parse kubernetes external lb and return null");
            return null;
        }

        return DefaultKubernetesExternalLb.builder().serviceName(serviceName)
                .loadBalancerIp(IpAddress.valueOf(lbIp))
                .servicePorts(servicePorts)
                .endpointSet(endpointSet)
                .loadBalancerGwIp(loadbalancerGatewayIp)
                .loadBalancerGwMac(loadBalancerGatewayMac)
                .build();
    }

    private IpAddress loadBalancerGatewayIp() {
        KubernetesExternalLbConfig config = lbConfigService.lbConfigs().stream().findAny().orElse(null);

        if (config == null) {
            return null;
        }

        return config.loadBalancerGwIp();
    }

    private MacAddress loadBalancerGatewayMac() {
        KubernetesExternalLbConfig config = lbConfigService.lbConfigs().stream().findAny().orElse(null);

        if (config == null) {
            return null;
        }

        return config.loadBalancerGwMac();
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
                case KUBEVIRT_NODE_REMOVED:
                    eventExecutor.execute(() -> processNodeDeletion(event.subject()));
                    break;
                case KUBEVIRT_NODE_UPDATED:
                default:
                    // do nothing
                    break;
            }
        }

        private void processNodeCompletion(KubevirtNode node) {
            if (!isRelevantHelper()) {
                return;
            }
            addOrUpdateExternalLoadBalancers();
        }

        private void processNodeDeletion(KubevirtNode node) {
            if (!isRelevantHelper()) {
                return;
            }
            addOrUpdateExternalLoadBalancers();
        }
    }
}
