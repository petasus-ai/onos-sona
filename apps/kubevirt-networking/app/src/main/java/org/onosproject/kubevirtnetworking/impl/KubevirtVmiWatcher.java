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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.onosproject.kubevirtnetworking.api.KubevirtNetworkAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtPort;
import org.onosproject.kubevirtnetworking.api.KubevirtPortAdminService;
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

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.getPorts;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes VMI watcher used for feeding VMI information.
 */
@Component(immediate = true)
public class KubevirtVmiWatcher {

    private final Logger log = getLogger(getClass());

    private static final String STATUS = "status";
    private static final String NODE_NAME = "nodeName";
    private static final String METADATA = "metadata";
    private static final String NAME = "name";
    private static final String INTERFACES = "interfaces";
    private static final String MAC = "mac";
    private static final String DEFAULT = "default";

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected MastershipService mastershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LeadershipService leadershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNodeService nodeService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtNetworkAdminService networkAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtPortAdminService portAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService configService;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));

    private static final long RECONNECT_DELAY_S = 5;

    private static final int DEVICE_ID_RETRY_LIMIT = 10;
    private static final long DEVICE_ID_RETRY_DELAY_S = 3;

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

    // set while WE close the watch on purpose (re-instantiation, shutdown);
    // an onClose callback fired by that intentional close must not schedule
    // another re-instantiation, or the watch would flap forever
    private volatile boolean closingWatch;

    private final InternalKubevirtVmiWatcher watcher = new InternalKubevirtVmiWatcher();
    private final InternalKubevirtApiConfigListener
            configListener = new InternalKubevirtApiConfigListener();

    CustomResourceDefinitionContext vmiCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Namespaced")
            .withVersion("v1")
            .withPlural("virtualmachineinstances")
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
        eventExecutor.shutdown();
        closeWatch();
        closeWatchClient();

        log.info("Stopped");
    }

    private synchronized void instantiateWatcher() {
        closeWatch();
        closeWatchClient();
        watchClient = k8sClient(configService);

        if (watchClient == null) {
            scheduleReconnect();
            return;
        }

        try {
            watch = watchClient.customResource(vmiCrdCxt).watch(watcher);
        } catch (Exception e) {
            log.error("Failed to instantiate watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
        }
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

    private class InternalKubevirtVmiWatcher implements Watcher<String> {

        @Override
        public void eventReceived(Action action, String s) {
            switch (action) {
                case ADDED:
                case MODIFIED:
                    eventExecutor.execute(() -> processAddition(s));
                    break;
                case ERROR:
                    log.warn("Failures processing VM manipulation.");
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

        private void processAddition(String resource) {
            processAddition(resource, 0);
        }

        // The initial watch replay races the other watchers: the node,
        // network or port this VMI refers to may not have reached its store
        // yet. This event is the only carrier of the port's device ID, so a
        // one-shot drop here leaves the port device-less forever and every
        // per-port flow (ACL redirects, SG entries, ...) silently unprogrammed.
        // Retry with a bounded schedule instead of skipping or sleeping on the
        // shared event executor.
        private void processAddition(String resource, int attempt) {
            if (!isMaster()) {
                return;
            }

            String nodeName = parseNodeName(resource);
            String vmiName = parseVmiName(resource);

            if (nodeName == null) {
                return;
            }

            if (nodeService.node(nodeName) == null) {
                retryAddition(resource, vmiName, attempt,
                        "node " + nodeName + " is not synced yet");
                return;
            }

            Set<KubevirtPort> ports = getPorts(nodeService,
                                        networkAdminService.networks(), resource);

            if (ports.size() == 0) {
                if (!hasUsableInterface(resource)) {
                    // virt-handler fills status.interfaces only after the
                    // domain is up, and the retries re-parse this frozen
                    // event snapshot, so waiting can never make the entries
                    // appear; the MODIFIED event that carries the interfaces
                    // re-enters this handler and completes the update
                    log.debug("Skipping the device ID update of VMI {}: " +
                            "status carries no usable interface yet", vmiName);
                    return;
                }
                // the interfaces are present but match no known network:
                // either the network store is not synced yet, or the NIC is
                // not SONA-managed at all; the latter just lets the bounded
                // retries expire without any effect
                retryAddition(resource, vmiName, attempt,
                        "no interface matches a known network");
                return;
            }

            boolean portMissing = false;

            for (KubevirtPort port : ports) {
                KubevirtPort existing = portAdminService.port(port.macAddress());

                if (existing == null) {
                    // the VM watcher has not created the port yet
                    portMissing = true;
                    continue;
                }

                if (port.deviceId() != null) {
                    if (existing.deviceId() == null || !existing.deviceId().equals(port.deviceId())) {
                        KubevirtPort updated = existing.updateDeviceId(port.deviceId());
                        // internally we update device ID of kubevirt port
                        portAdminService.updatePort(updated);
                    }
                }
            }

            if (portMissing) {
                retryAddition(resource, vmiName, attempt,
                        "port not created by the VM watcher yet");
            }
        }

        private void retryAddition(String resource, String vmiName,
                                   int attempt, String reason) {
            if (attempt >= DEVICE_ID_RETRY_LIMIT) {
                log.warn("Giving up the device ID update of VMI {} after {} " +
                        "attempts: {}", vmiName, attempt, reason);
                return;
            }

            log.debug("Retrying the device ID update of VMI {} in {}s: {}",
                    vmiName, DEVICE_ID_RETRY_DELAY_S, reason);
            reconnectExecutor.schedule(() -> eventExecutor.execute(() ->
                    processAddition(resource, attempt + 1)),
                    DEVICE_ID_RETRY_DELAY_S, TimeUnit.SECONDS);
        }

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        // tells whether the VMI snapshot carries at least one interface entry
        // that getPorts() can turn into a port: a non-default name plus a MAC;
        // before the domain is up the status has no such entry, and retrying
        // on the frozen snapshot can never change that
        private boolean hasUsableInterface(String resource) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode statusJson = json.get(STATUS);
                JsonNode interfacesJson = statusJson == null ?
                        null : statusJson.get(INTERFACES);
                if (interfacesJson == null) {
                    return false;
                }
                for (JsonNode intf : interfacesJson) {
                    JsonNode nameJson = intf.get(NAME);
                    if (nameJson != null && !DEFAULT.equals(nameJson.asText())
                            && intf.get(MAC) != null) {
                        return true;
                    }
                }
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VMI interfaces");
            }
            return false;
        }

        private String parseVmiName(String resource) {
            String vmiName = null;

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode metadataJson = json.get(METADATA);
                JsonNode vmiNameJson = metadataJson.get(NAME);
                vmiName = vmiNameJson != null ? vmiNameJson.asText() : null;
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VMI name");
            }

            return vmiName;
        }

        private String parseNodeName(String resource) {
            String nodeName = null;
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode statusJson = json.get(STATUS);
                JsonNode nodeNameJson = statusJson.get(NODE_NAME);
                nodeName = nodeNameJson != null ? nodeNameJson.asText() : null;
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VMI nodename");
            }

            return nodeName;
        }
    }
}
