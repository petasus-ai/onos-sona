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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
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

    // the client owning the currently active watch; closing it terminates the
    // watch, so keeping exactly one instance prevents both duplicated watches
    // and client (thread/connection pool) leaks on re-instantiation
    private KubernetesClient watchClient;

    // the handle of the active watch; it MUST be closed before its client,
    // otherwise the fabric8 watch manager keeps re-running its reconnect
    // loop on top of the client's terminated dispatcher, flooding the log
    // with "Exec Failure ... executor rejected" warnings
    private Watch watch;

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
            watch = watchClient.customResource(vmCrdCxt).watch(watcher);
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

    private class InternalKubevirtVmWatcher implements Watcher<String> {

        @Override
        public void eventReceived(Action action, String resource) {
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
            // the watch dies on API server restarts, resourceVersion expiry
            // (HTTP 410) and fabric8 bugs; re-watch after a short delay so a
            // down API server does not turn this into a tight reconnect loop
            log.warn("Watcher closed, re-instantiating in {}s",
                    RECONNECT_DELAY_S, e);
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
                    if (!existing.ipAddress().equals(ip)) {
                        updatedPort = updatedPort.updateIpAddress(ip);
                    }
                    if (!existing.securityGroups().equals(sgs) ||
                            !existing.ipAddress().equals(ip)) {
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
                    String ip = intf.get(IP).asText();
                    result.put(network, IpAddress.valueOf(ip));
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

