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
package org.onosproject.kubevirtnode.impl;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigEvent;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigListener;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeAdminService;
import org.onosproject.mastership.MastershipService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.MASTER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.onosproject.kubevirtnode.api.KubevirtNodeService.APP_ID;
import static org.onosproject.kubevirtnode.api.KubevirtNodeState.INIT;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.buildKubevirtNode;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.k8sClient;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes node watcher used for feeding node information.
 */
@Component(immediate = true)
public class KubevirtNodeWatcher {

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
    protected KubevirtNodeAdminService kubevirtNodeAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService kubevirtApiConfigService;

    private static final long RECONNECT_DELAY_S = 5;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));
    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));
    private final Watcher<Node> internalKubevirtNodeWatcher = new InternalKubevirtNodeWatcher();
    private final InternalKubevirtApiConfigListener
            internalKubevirtApiConfigListener = new InternalKubevirtApiConfigListener();

    private ApplicationId appId;
    private NodeId localNodeId;

    // the client owning the currently active watch; closing it terminates the
    // watch, so keeping exactly one instance prevents both duplicated watches
    // and client (thread/connection pool) leaks on re-instantiation
    private KubernetesClient client;

    // the handle of the active watch; it MUST be closed before its client,
    // otherwise the fabric8 watch manager keeps re-running its reconnect
    // loop on top of the client's terminated dispatcher, flooding the log
    // with "Exec Failure ... executor rejected" warnings
    private Watch watch;

    // set while WE close the watch on purpose (re-instantiation, shutdown);
    // an onClose callback fired by that intentional close must not schedule
    // another re-instantiation, or the watch would flap forever
    private volatile boolean closingWatch;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());
        kubevirtApiConfigService.addListener(internalKubevirtApiConfigListener);

        // a restarted instance never sees the API config UPDATED event again,
        // so establish the watch from the current config as well; every
        // instance keeps a watch and the leader check in the event handlers
        // decides who processes the events, which also covers leadership moves
        eventExecutor.execute(this::instantiateNodeWatcher);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        kubevirtApiConfigService.removeListener(internalKubevirtApiConfigListener);
        leadershipService.withdraw(appId.name());
        reconnectExecutor.shutdown();
        eventExecutor.shutdown();
        closeWatch();
        closeClient();

        log.info("Stopped");
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

    private synchronized void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private synchronized void instantiateNodeWatcher() {
        closeWatch();
        KubevirtApiConfig config = kubevirtApiConfigService.apiConfig();
        if (config == null) {
            return;
        }

        closeClient();
        client = k8sClient(config);

        if (client == null) {
            scheduleReconnect();
            return;
        }

        try {
            // re-list before watching: node events that occurred while no
            // watch was active (restart, reconnect window) are replayed here;
            // additions/updates are idempotent, missed deletions are left to
            // the operator rather than mass-removing nodes on a bad list
            client.nodes().list().getItems().forEach(node -> {
                internalKubevirtNodeWatcher.eventReceived(Watcher.Action.ADDED, node);
                internalKubevirtNodeWatcher.eventReceived(Watcher.Action.MODIFIED, node);
            });
            watch = client.nodes().watch(internalKubevirtNodeWatcher);
        } catch (Exception e) {
            log.error("Failed to watch kubernetes nodes, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        reconnectExecutor.schedule(this::instantiateNodeWatcher,
                RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    private class InternalKubevirtApiConfigListener implements KubevirtApiConfigListener {

        private boolean isRelevantHelper() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        @Override
        public void event(KubevirtApiConfigEvent event) {

            switch (event.type()) {
                case KUBEVIRT_API_CONFIG_UPDATED:
                    eventExecutor.execute(this::processConfigUpdating);
                    break;
                case KUBEVIRT_API_CONFIG_CREATED:
                case KUBEVIRT_API_CONFIG_REMOVED:
                default:
                    // do nothing
                    break;
            }
        }

        private void processConfigUpdating() {
            if (!isRelevantHelper()) {
                return;
            }

            instantiateNodeWatcher();
        }
    }

    private class InternalKubevirtNodeWatcher implements Watcher<Node> {

        @Override
        public void eventReceived(Action action, Node node) {
            switch (action) {
                case ADDED:
                    eventExecutor.execute(() -> processAddition(node));
                    break;
                case MODIFIED:
                    eventExecutor.execute(() -> processModification(node));
                    break;
                case DELETED:
                    eventExecutor.execute(() -> processDeletion(node));
                    break;
                case ERROR:
                    log.warn("Failures processing node manipulation.");
                    break;
                default:
                    // do nothing
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
            // (HTTP 410) and fabric8 bugs; re-list and re-watch after a short
            // delay so events missed during the gap are replayed and a down
            // API server does not turn this into a tight reconnect loop
            log.warn("Node watcher closed, re-instantiating in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }

        private void processAddition(Node node) {
            if (!isMaster()) {
                return;
            }

            KubevirtNode kubevirtNode = buildKubevirtNode(node);
            log.info("buildKubevirtNode: {}", kubevirtNode);
            if (kubevirtNode.type() == WORKER || kubevirtNode.type() == GATEWAY) {
                if (!kubevirtNodeAdminService.hasNode(kubevirtNode.hostname())) {
                    kubevirtNodeAdminService.createNode(kubevirtNode);
                }
            }
        }

        private void processModification(Node node) {
            if (!isMaster()) {
                return;
            }

            log.trace("Process node {} updating event from API server.",
                    node.getMetadata().getName());

            KubevirtNode original = buildKubevirtNode(node);
            KubevirtNode existing = kubevirtNodeAdminService.node(node.getMetadata().getName());

            // if a master node is annotated as a gateway node, we simply add
            // the node into the cluster
            if (original.type() == GATEWAY && existing == null) {
                kubevirtNodeAdminService.createNode(original);
            }

            // if a gateway annotation removed from the master node, we simply remove
            // the node from the cluster
            if (original.type() == MASTER && existing != null && existing.type() == GATEWAY) {
                kubevirtNodeAdminService.removeNode(original.hostname());
            }

            if (existing != null) {
                // we update the kubevirt node and re-run bootstrapping,
                // if the updated node has different phyInts and data IP
                // this means we assume that the node's hostname, type and mgmt IP
                // are immutable
                if (!original.phyIntfs().equals(existing.phyIntfs()) ||
                        !original.dataIp().equals(existing.dataIp())) {
                    kubevirtNodeAdminService.updateNode(original.updateState(INIT));
                }
            }
        }

        private void processDeletion(Node node) {
            if (!isMaster()) {
                return;
            }

            log.trace("Process node {} removal event from API server.",
                    node.getMetadata().getName());

            KubevirtNode existing = kubevirtNodeAdminService.node(node.getMetadata().getName());

            if (existing != null) {
                kubevirtNodeAdminService.removeNode(node.getMetadata().getName());
            }
        }

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }
    }
}
