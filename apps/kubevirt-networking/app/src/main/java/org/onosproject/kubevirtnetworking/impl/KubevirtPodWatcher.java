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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.KubevirtPodAdminService;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes pod watcher used for feeding pod information.
 */
@Component(immediate = true)
public class KubevirtPodWatcher {

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
    protected KubevirtPodAdminService kubevirtPodAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService kubevirtApiConfigService;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));

    private static final long RECONNECT_DELAY_S = 5;

    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));

    // the client owning the currently active watch; closing it terminates the
    // watch, so keeping exactly one instance prevents both duplicated watches
    // and client (thread/connection pool) leaks on re-instantiation
    private KubernetesClient watchClient;

    // the handle of the active watch; it MUST be closed before its client,
    // otherwise the fabric8 watch manager keeps re-running its reconnect
    // loop on top of the client's terminated dispatcher, flooding the log
    // with "Exec Failure ... executor rejected" warnings
    private Watch watch;
    private final Watcher<Pod> internalKubevirtPodWatcher = new InternalKubevirtPodWatcher();
    private final InternalKubevirtApiConfigListener
            internalKubevirtApiConfigListener = new InternalKubevirtApiConfigListener();

    private ApplicationId appId;
    private NodeId localNodeId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(KUBEVIRT_NETWORKING_APP_ID);
        localNodeId = clusterService.getLocalNode().id();
        leadershipService.runForLeadership(appId.name());
        kubevirtApiConfigService.addListener(internalKubevirtApiConfigListener);

        // a restarted instance never sees the API config UPDATED event again,
        // so establish the watch from the current config as well; every
        // instance keeps a watch and the leader check in the event handlers
        // decides who processes the events, which also covers leadership moves
        eventExecutor.execute(this::instantiatePodWatcher);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        kubevirtApiConfigService.removeListener(internalKubevirtApiConfigListener);
        leadershipService.withdraw(appId.name());
        reconnectExecutor.shutdown();
        eventExecutor.shutdown();
        closeWatch();
        closeWatchClient();

        log.info("Stopped");
    }

    private synchronized void instantiatePodWatcher() {
        closeWatch();
        closeWatchClient();
        watchClient = k8sClient(kubevirtApiConfigService);

        if (watchClient == null) {
            scheduleReconnect();
            return;
        }

        try {
            watch = watchClient.pods().inAnyNamespace().watch(internalKubevirtPodWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }
    }

    private synchronized void closeWatch() {
        if (watch != null) {
            try {
                watch.close();
            } catch (Exception e) {
                log.debug("Failed to close the previous watch", e);
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
        reconnectExecutor.schedule(this::instantiatePodWatcher,
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

            instantiatePodWatcher();
        }
    }

    private class InternalKubevirtPodWatcher implements Watcher<Pod> {

        @Override
        public void eventReceived(Action action, Pod pod) {
            switch (action) {
                case ADDED:
                    eventExecutor.execute(() -> processAddition(pod));
                    break;
                case MODIFIED:
                    eventExecutor.execute(() -> processModification(pod));
                    break;
                case DELETED:
                    eventExecutor.execute(() -> processDeletion(pod));
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
            // the watch dies on API server restarts, resourceVersion expiry
            // (HTTP 410) and fabric8 bugs; re-watch after a short delay so a
            // down API server does not turn this into a tight reconnect loop
            log.warn("Watcher closed, re-instantiating in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }

        private void processAddition(Pod pod) {
            if (!isMaster() || !isDefaultNs(pod)) {
                return;
            }

            log.trace("Process pod {} creating event from API server.",
                    pod.getMetadata().getName());

            if (kubevirtPodAdminService.pod(pod.getMetadata().getUid()) == null) {
                kubevirtPodAdminService.createPod(pod);
            }
        }

        private void processModification(Pod pod) {
            if (!isMaster() || !isDefaultNs(pod)) {
                return;
            }

            log.trace("Process pod {} updating event from API server.",
                    pod.getMetadata().getName());

            if (kubevirtPodAdminService.pod(pod.getMetadata().getUid()) != null) {
                kubevirtPodAdminService.updatePod(pod);
            }
        }

        private void processDeletion(Pod pod) {
            if (!isMaster() || !isDefaultNs(pod)) {
                return;
            }

            log.trace("Process pod {} removal event from API server.",
                    pod.getMetadata().getName());

            kubevirtPodAdminService.removePod(pod.getMetadata().getUid());
        }

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        private boolean isDefaultNs(Pod pod) {
            return pod.getMetadata().getNamespace().equals("default");
        }
    }
}
