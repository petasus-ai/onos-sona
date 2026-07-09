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
package org.onosproject.kubevirtnode.impl;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import org.onosproject.kubevirtnode.api.DefaultKubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigAdminService;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigEvent;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigListener;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNodeService;
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
import static org.onosproject.kubevirtnode.api.KubevirtNodeService.APP_ID;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.k8sClient;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes configmap watcher used for external loadbalancing among PODs.
 */
@Component(immediate = true)
public class KubernetesConfigMapWatcher {
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
    protected KubevirtApiConfigService configService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubernetesExternalLbConfigAdminService adminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeService nodeService;

    private static final String KUBE_DASH_VIP = "kube-vip";
    private static final String KUBE_VIP = "kubevip";
    private static final String LOADBALANCER_IP = "loadBalancerIP";
    private static final String TYPE_LOADBALANCER = "LoadBalancer";
    private static final String KUBE_SYSTEM = "kube-system";
    private static final String GATEWAY_IP = "gateway-ip";
    private static final String GATEWAY_MAC = "gateway-mac";
    private static final String RANGE_GLOBAL = "range-global";

    private ApplicationId appId;
    private NodeId localNodeId;

    private static final long RECONNECT_DELAY_S = 5;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));
    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));

    private final InternalKubevirtApiConfigListener
            configListener = new InternalKubevirtApiConfigListener();

    private final InternalKubernetesConfigMapWatcher
            mapWatcher = new InternalKubernetesConfigMapWatcher();

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

    private synchronized void instantiateWatcher() {
        closeWatch();
        KubevirtApiConfig config = configService.apiConfig();
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
            // re-list before watching so that a config map change that
            // happened while no watch was active is replayed
            ConfigMap existing = client.configMaps()
                    .inNamespace(KUBE_SYSTEM).withName(KUBE_VIP).get();
            if (existing != null) {
                mapWatcher.eventReceived(Watcher.Action.MODIFIED, existing);
            }
            watch = client.configMaps().inNamespace(KUBE_SYSTEM).withName(KUBE_VIP).watch(mapWatcher);
        } catch (Exception e) {
            log.error("Failed to watch kube-vip config map, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        reconnectExecutor.schedule(this::instantiateWatcher,
                RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    private class InternalKubernetesConfigMapWatcher implements Watcher<ConfigMap> {

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }


        @Override
        public void eventReceived(Action action, ConfigMap configMap) {
            switch (action) {
                case ADDED:
                    log.info("ConfigMap event ADDED received");
                    eventExecutor.execute(() -> processAddOrMod(configMap));
                    break;
                case MODIFIED:
                    log.info("ConfigMap event MODIFIED received");
                    eventExecutor.execute(() -> processAddOrMod(configMap));
                    break;
                case DELETED:
                    log.info("ConfigMap event DELETED received");
                    eventExecutor.execute(() -> processDeletion(configMap));
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
            // (HTTP 410) and fabric8 bugs; re-read and re-watch after a short
            // delay so changes missed during the gap are replayed and a down
            // API server does not turn this into a tight reconnect loop
            if (e != null && e.isHttpGone()) {
                // expected 410: our (stale) resourceVersion aged out of the
                // API server watch cache; the reconnect re-lists from a fresh
                // version, so log it without the noisy stack trace
                log.info("Configmap watcher expired (too old resource version), " +
                        "re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Configmap watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleReconnect();
        }

        private void processAddOrMod(ConfigMap configMap) {
            if (configMap == null || !isMaster()) {
                return;
            }

            String configName = configMap.getMetadata().getName();
            if (!configName.equals(KUBE_VIP)) {
                return;
            }

            KubernetesExternalLbConfig lbConfig = parseKubernetesExternalLbConfig(configMap, configName);

            if (lbConfig == null) {
                return;
            }

            log.info("Kubernetes external LB config inserted/updated {}", lbConfig);

            if (adminService.lbConfig(configName) == null) {
                adminService.createKubernetesExternalLbConfig(lbConfig);
            } else {
                adminService.updateKubernetesExternalLbConfig(lbConfig);
            }
        }

        private void processDeletion(ConfigMap configMap) {
            if (configMap == null || !isMaster()) {
                return;
            }

            String configName = configMap.getMetadata().getName();
            if (!configName.equals(KUBE_VIP)) {
                return;
            }

            KubernetesExternalLbConfig lbConfig = adminService.lbConfig(configName);

            if (lbConfig == null) {
                return;
            }

            adminService.removeKubernetesExternalLbConfig(configName);
        }

        private KubernetesExternalLbConfig parseKubernetesExternalLbConfig(ConfigMap configMap, String configName) {

            if (configMap == null || configMap.getData() == null ||
                    configMap.getData().get(GATEWAY_IP) == null || configMap.getData().get(RANGE_GLOBAL) == null) {
                return null;
            }

            KubernetesExternalLbConfig.Builder lbConfigBuilder = DefaultKubernetesExternalLbConfig.builder();

            try {
                lbConfigBuilder.configName(configName)
                        .loadBalancerGwIp(IpAddress.valueOf(configMap.getData().get(GATEWAY_IP)))
                        .globalIpRange(configMap.getData().get(RANGE_GLOBAL));

                if (configMap.getData().containsKey(GATEWAY_MAC)) {
                    lbConfigBuilder.loadBalancerGwMac(MacAddress.valueOf(configMap.getData().get(GATEWAY_MAC)));
                }

            } catch (IllegalArgumentException e) {
                log.error("Failed to parse kubernetes external LB config", e);
                return null;
            }

            return lbConfigBuilder.build();
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
        }
    }
}
