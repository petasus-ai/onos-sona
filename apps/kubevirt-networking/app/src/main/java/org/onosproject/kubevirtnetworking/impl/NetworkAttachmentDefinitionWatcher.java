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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkAdminService;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigEvent;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigListener;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
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
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.liveResourceKeys;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseKubevirtNetwork;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseResourceName;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes network-attachment-definition watcher used for feeding kubevirt network information.
 */
@Component(immediate = true)
public class NetworkAttachmentDefinitionWatcher {

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
    protected KubevirtNetworkAdminService adminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService configService;

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

    private final InternalNetworkAttachmentDefinitionWatcher
            watcher = new InternalNetworkAttachmentDefinitionWatcher();
    private final InternalKubevirtApiConfigListener
            configListener = new InternalKubevirtApiConfigListener();

    CustomResourceDefinitionContext nadCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("k8s.cni.cncf.io")
            .withScope("Namespaced")
            .withVersion("v1")
            .withPlural("network-attachment-definitions")
            .build();

    private ApplicationId appId;
    private NodeId localNodeId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(KUBEVIRT_NETWORKING_APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());
        configService.addListener(configListener);

        // a restarted instance never sees the API config UPDATED event again,
        // so establish the watch from the current config as well; every
        // instance keeps a watch and the leader check in the event handlers
        // decides who processes the events, which also covers leadership moves
        eventExecutor.execute(this::instantiateWatcher);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        configService.removeListener(configListener);
        leadershipService.withdraw(appId.name());
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
        KubernetesClient client = k8sClient(configService);
        watchClient = client;

        if (client == null) {
            scheduleReconnect();
            return;
        }

        try {
            watch = client.customResource(nadCrdCxt).watch(watcher);
        } catch (Exception e) {
            log.error("Failed to instantiate watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
            return;
        }

        // the watch above carries no resourceVersion, so the API server replays
        // every current network as ADDED (processed as an upsert); it never
        // reports networks deleted while the watch was down, so reconcile the
        // store against a fresh listing to prune those strays
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
     * Reconciles the network store against the API server, removing networks
     * that were deleted while this watch was down. Runs only on the leader and
     * only when the full listing parses cleanly, so a partial view can never
     * delete live state.
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

        // snapshot the store keys before listing: a network created between the
        // snapshot and the list carries a still-in-flight ADDED and must not be
        // mistaken for a stray
        Set<String> storedIds = adminService.networks().stream()
                .map(KubevirtNetwork::networkId)
                .collect(Collectors.toSet());

        Set<String> liveIds = liveResourceKeys(client, nadCrdCxt, resource -> {
            KubevirtNetwork network = parseKubevirtNetwork(resource);
            return network == null ? null : network.networkId();
        });

        if (liveIds == null) {
            log.debug("Skipping network resync: incomplete listing");
            return;
        }

        storedIds.stream()
                .filter(id -> !liveIds.contains(id))
                .forEach(id -> {
                    log.info("Pruning stale network {} absent from API server", id);
                    try {
                        adminService.removeNetwork(id);
                    } catch (Exception e) {
                        log.warn("Failed to prune stale network {}", id, e);
                    }
                });
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
        }
    }

    private class InternalNetworkAttachmentDefinitionWatcher
            implements Watcher<String> {

        @Override
        public void eventReceived(Action action, String resource) {
            switch (action) {
                case ADDED:
                    eventExecutor.execute(() -> processAddition(resource));
                    break;
                case MODIFIED:
                    eventExecutor.execute(() -> processModification(resource));
                    break;
                case DELETED:
                    eventExecutor.execute(() -> processDeletion(resource));
                    break;
                case ERROR:
                    log.warn("Failures processing network-attachment-definition manipulation.");
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
                // version, so log it without the noisy stack trace
                log.info("Watcher expired (too old resource version), " +
                        "re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleReconnect();
        }

        private void processAddition(String resource) {
            if (!isMaster()) {
                return;
            }

            String name = parseResourceName(resource);

            log.trace("Process NetworkAttachmentDefinition {} creating event from API server.",
                    name);

            KubevirtNetwork network = parseKubevirtNetwork(resource);
            if (network != null) {
                if (adminService.network(network.networkId()) == null) {
                    adminService.createNetwork(network);
                } else {
                    // on a resync the API server re-delivers existing networks
                    // as ADDED; upsert so one changed while the watch was down
                    // is not silently kept stale
                    adminService.updateNetwork(network);
                }
            }
        }

        private void processModification(String resource) {
            if (!isMaster()) {
                return;
            }

            String name = parseResourceName(resource);

            log.trace("Process NetworkAttachmentDefinition {} updating event from API server.",
                    name);

            KubevirtNetwork network = parseKubevirtNetwork(resource);
            if (network != null) {
                adminService.updateNetwork(network);
            }
        }

        private void processDeletion(String resource) {
            if (!isMaster()) {
                return;
            }

            String name = parseResourceName(resource);

            log.trace("Process NetworkAttachmentDefinition {} removal event from API server.",
                    name);

            if (adminService.network(name) != null) {
                adminService.removeNetwork(name);
            }
        }

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }
    }
}
