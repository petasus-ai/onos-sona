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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.apache.commons.lang3.StringUtils;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.LeadershipService;
import org.onosproject.cluster.NodeId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.DefaultKubevirtPort;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.customResourceJson;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.liveResourceKeySets;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubernetes VM watcher used for feeding VM information.
 */
@Component(immediate = true)
public class KubevirtVmWatcher {

    private final Logger log = getLogger(getClass());

    private static final String SPEC = "spec";
    private static final String TEMPLATE = "template";
    private static final String METADATA = "metadata";
    private static final String NAMESPACE = "namespace";

    private static final String ANNOTATIONS = "annotations";
    private static final String DOMAIN = "domain";
    private static final String DEVICES = "devices";
    private static final String INTERFACES = "interfaces";
    private static final String SECURITY_GROUPS = "securityGroups";
    private static final String NAME = "name";
    private static final String NETWORK = "network";
    private static final String MAC = "macAddress";
    private static final String SRIOV = "sriov";
    private static final String IP = "ipAddress";
    private static final String DEFAULT = "default";
    private static final String CNI_ZERO = "cni0";
    private static final String NETWORK_SUFFIX = "-net";

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

    private final InternalKubevirtVmWatcher watcher = new InternalKubevirtVmWatcher();
    private final InternalKubevirtApiConfigListener
            configListener = new InternalKubevirtApiConfigListener();

    CustomResourceDefinitionContext vmCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Namespaced")
            .withVersion("v1")
            .withPlural("virtualmachines")
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
            watch = client.genericKubernetesResources(vmCrdCxt).inAnyNamespace().watch(watcher);
        } catch (Exception e) {
            log.error("Failed to instantiate watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleReconnect();
            return;
        }

        // the watch above carries no resourceVersion, so the API server replays
        // every current VM as ADDED; it never reports VMs deleted while the
        // watch was down, so reconcile the port store against a fresh listing to
        // prune the ports of VMs that are gone
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
     * Reconciles the port store against the API server, removing the ports of
     * VMs deleted while this watch was down. Ports are created only from VM
     * interfaces, so a port whose MAC is backed by no live VM is a stray.
     * Runs only on the leader and only when the full listing parses cleanly, so
     * a partial view can never delete live state.
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

        // snapshot the port keys before listing: a port whose VM ADDED is still
        // in flight must not be mistaken for a stray
        Set<MacAddress> storedMacs = portAdminService.ports().stream()
                .map(KubevirtPort::macAddress)
                .collect(Collectors.toSet());

        Set<String> liveMacs = liveResourceKeySets(client, vmCrdCxt, this::parseMacsForResync);
        if (liveMacs == null) {
            log.debug("Skipping VM port resync: incomplete listing");
            return;
        }

        storedMacs.stream()
                .filter(mac -> !liveMacs.contains(mac.toString()))
                .forEach(mac -> {
                    log.info("Pruning stale port {} whose VM is absent from API server", mac);
                    try {
                        portAdminService.removePort(mac);
                    } catch (Exception e) {
                        log.warn("Failed to prune stale port {}", mac, e);
                    }
                });
    }

    /**
     * Derives the MAC store keys of a VM resource for resync, mirroring the
     * filter of {@code parseMacAddresses} (skip the default interface, require a
     * MAC, skip SR-IOV). Returns the MAC strings, an empty set for a VM with no
     * eligible interface, or null if the resource cannot be parsed so the caller
     * skips pruning rather than delete live ports.
     *
     * @param resource raw VM resource JSON
     * @return the MAC store keys, or null when the resource cannot be parsed
     */
    private Set<String> parseMacsForResync(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            JsonNode spec = json.get(SPEC).get(TEMPLATE).get(SPEC);
            ArrayNode interfaces = (ArrayNode) spec.get(DOMAIN).get(DEVICES).get(INTERFACES);
            if (interfaces == null) {
                // a VM with no network backs no port
                return new HashSet<>();
            }

            Set<String> macs = new HashSet<>();
            for (JsonNode intf : interfaces) {
                String intfName = intf.get(NAME).asText();
                JsonNode macJson = intf.get(MAC);
                JsonNode sriov = intf.get(SRIOV);
                if (!StringUtils.equals(DEFAULT, intfName) && macJson != null && sriov == null) {
                    macs.add(MacAddress.valueOf(macJson.asText()).toString());
                }
            }
            return macs;
        } catch (Exception e) {
            log.warn("Failed to parse VM MAC addresses for resync", e);
            return null;
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

    private class InternalKubevirtVmWatcher implements Watcher<GenericKubernetesResource> {

        @Override
        public void eventReceived(Action action, GenericKubernetesResource object) {
            String resource = customResourceJson(object);
            switch (action) {
                case ADDED:
                    eventExecutor.execute(() -> processAddition(resource));
                    break;
                case DELETED:
                    eventExecutor.execute(() -> processDeletion(resource));
                    break;
                case MODIFIED:
                    eventExecutor.execute(() -> processModification(resource));
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
            if (!isMaster()) {
                return;
            }

            String vmName = parseVmName(resource);

            parseMacAddresses(resource).forEach((mac, net) -> {
                KubevirtPort port = DefaultKubevirtPort.builder()
                        .vmName(vmName)
                        .macAddress(mac)
                        .networkId(net)
                        .build();

                Set<String> sgs = parseSecurityGroups(resource);
                port = port.updateSecurityGroups(sgs);

                Map<String, IpAddress> ips = parseIpAddresses(resource);
                IpAddress ip = ips.get(port.networkId());

                port = port.updateIpAddress(ip);

                if (portAdminService.port(port.macAddress()) == null) {
                    portAdminService.createPort(port);
                }
            });
        }

        private void processModification(String resource) {
            if (!isMaster()) {
                return;
            }

            String vmName = parseVmName(resource);

            parseMacAddresses(resource).forEach((mac, net) -> {
                KubevirtPort port = DefaultKubevirtPort.builder()
                        .vmName(vmName)
                        .macAddress(mac)
                        .networkId(net)
                        .build();

                KubevirtPort existing = portAdminService.port(port.macAddress());
                Set<String> sgs = parseSecurityGroups(resource);

                Map<String, IpAddress> ips = parseIpAddresses(resource);
                IpAddress ip = ips.get(port.networkId());

                if (existing == null) {
                    // if the network related information is filled with VM update event,
                    // and there is no port found in the store
                    // we try to add port by extracting network related info from VM
                    port = port.updateSecurityGroups(sgs);
                    port = port.updateIpAddress(ip);
                    portAdminService.createPort(port);
                } else {
                    // we only update the port, if either the newly updated
                    // security groups have different values compared to existing
                    // ones or the newly updated IP address has been changed
                    KubevirtPort updatedPort = existing;
                    if (!existing.securityGroups().equals(sgs)) {
                        updatedPort = updatedPort.updateSecurityGroups(sgs);
                    }
                    if (!Objects.equals(existing.ipAddress(), ip)) {
                        updatedPort = updatedPort.updateIpAddress(ip);
                    }
                    if (!existing.securityGroups().equals(sgs) ||
                            !Objects.equals(existing.ipAddress(), ip)) {
                        portAdminService.updatePort(updatedPort);
                    }
                }
            });
        }

        private void processDeletion(String resource) {
            if (!isMaster()) {
                return;
            }

            parseMacAddresses(resource).forEach((mac, net) -> {
                KubevirtPort port = portAdminService.port(mac);
                if (port != null) {
                    portAdminService.removePort(mac);
                }
            });
        }

        private boolean isMaster() {
            return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
        }

        private String parseVmName(String resource) {
            String vmName = null;
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode nameJson = json.get(METADATA).get(NAME);
                if (nameJson != null) {
                    vmName = nameJson.asText();
                }
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VM name");
            }

            return vmName;
        }

        private Map<String, IpAddress> parseIpAddresses(String resource) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode metadata = json.get(SPEC).get(TEMPLATE).get(METADATA);

                JsonNode annots = metadata.get(ANNOTATIONS);
                if (annots == null) {
                    return new HashMap<>();
                }

                JsonNode interfacesJson = annots.get(INTERFACES);
                if (interfacesJson == null) {
                    return new HashMap<>();
                }

                Map<String, IpAddress> result = new HashMap<>();

                String interfacesString = interfacesJson.asText();
                ArrayNode interfaces = (ArrayNode) mapper.readTree(interfacesString);
                for (JsonNode intf : interfaces) {
                    String network = intf.get(NETWORK).asText();
                    JsonNode ipJson = intf.get(IP);
                    // SR-IOV fabric NICs (e.g. InfiniBand) are listed without
                    // an IP address; skip them so one IP-less entry does not
                    // abort port creation for the entire VM.
                    if (ipJson == null || ipJson.isNull()) {
                        continue;
                    }
                    result.put(network, IpAddress.valueOf(ipJson.asText()));
                }

                return result;
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VM IP addresses");
            }

            return new HashMap<>();
        }

        private Set<String> parseSecurityGroups(String resource) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode metadata = json.get(SPEC).get(TEMPLATE).get(METADATA);

                JsonNode annots = metadata.get(ANNOTATIONS);
                if (annots == null) {
                    return new HashSet<>();
                }

                JsonNode sgsJson = annots.get(SECURITY_GROUPS);
                if (sgsJson == null) {
                    return new HashSet<>();
                }

                Set<String> result = new HashSet<>();
                ArrayNode sgs = (ArrayNode) mapper.readTree(sgsJson.asText());
                for (JsonNode sg : sgs) {
                    result.add(sg.asText());
                }

                return result;

            } catch (IOException e) {
                log.error("Failed to parse kubevirt security group IDs.");
            }

            return new HashSet<>();
        }

        private Map<MacAddress, String> parseMacAddresses(String resource) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(resource);
                JsonNode metadata = json.get(METADATA);
                JsonNode spec = json.get(SPEC).get(TEMPLATE).get(SPEC);
                ArrayNode interfaces = (ArrayNode) spec.get(DOMAIN).get(DEVICES).get(INTERFACES);

                // if the VM is not associated with any network, we skip parsing MAC address
                if (interfaces == null) {
                    return ImmutableMap.of();
                }
                Map<MacAddress, String> result = new HashMap<>();
                for (JsonNode intf : interfaces) {
                    String intfName = intf.get(NAME).asText();
                    String namespace = metadata.get(NAMESPACE).asText();
                    String network = namespace + "/" + intfName;
                    JsonNode macJson = intf.get(MAC);
                    JsonNode sriov = intf.get(SRIOV);

                    if (!StringUtils.equals(DEFAULT, intfName) && macJson != null && sriov == null) {
                        String compact = StringUtils.substringBeforeLast(network, NETWORK_SUFFIX);
                        MacAddress mac = MacAddress.valueOf(macJson.asText());
                        result.put(mac, compact);
                    }
                }

                return result;
            } catch (IOException e) {
                log.error("Failed to parse kubevirt VM MAC addresses");
            }

            return new HashMap<>();
        }
    }
}

