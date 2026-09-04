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

import com.google.common.collect.ImmutableSet;
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
import org.onosproject.kubevirtnode.api.DefaultKubevirtNode;
import org.onosproject.kubevirtnode.api.DefaultKubevirtPhyInterface;
import org.onosproject.kubevirtnode.api.KubevirtApiConfig;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigEvent;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigListener;
import org.onosproject.kubevirtnode.api.KubevirtApiConfigService;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeAdminService;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.DeviceId;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.MASTER;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.onosproject.kubevirtnode.api.KubevirtNodeService.APP_ID;
import static org.onosproject.kubevirtnode.api.KubevirtNodeState.INIT;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.buildKubevirtNode;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.genDpidFromName;
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

    /**
     * Returns the values of the given physnet attribute (network name or
     * interface name) that more than one entry of the node's physnet-config
     * annotation carries.
     *
     * @param node      node being registered or updated
     * @param attribute physnet attribute to compare by
     * @return duplicated attribute values, empty if there are none
     */
    static Set<String> duplicatedPhysnetValues(KubevirtNode node,
                                               Function<KubevirtPhyInterface, String> attribute) {
        return node.phyIntfs().stream()
                .collect(Collectors.groupingBy(attribute, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the physnet bridge datapath ids of the given node that are
     * either declared twice within the node itself or already assigned to a
     * bridge (integration, tunnel or physnet) of another node.
     *
     * @param node   node being registered or updated
     * @param others nodes currently in the store
     * @return conflicting datapath ids, empty if there are none
     */
    /**
     * Returns the updated node with the physnet bridge datapath ids of its
     * stored copy carried over for every network whose annotation entry
     * carries no explicit phys_bridge_id.
     *
     * Without an explicit id the parser derives one from the network,
     * interface and host names, so re-pointing a network at another
     * interface silently gives its bridge a new datapath id. The bootstrap
     * then rewrites the id on the OVS bridge, which reconnects to ONOS as a
     * different device: every flow installed under the old id turns into a
     * dead store entry and a switch flow no device-scoped logic can see any
     * more, and the port-number keyed rules among them are never cleaned
     * up. A bridge's identity should follow the (node, network) pair it
     * serves, not the NIC that happens to be its uplink, so the id the
     * network already has is kept. An entry whose id is not the generated
     * one was set by the operator on purpose and is taken as is.
     *
     * @param updated  node built from the current annotation
     * @param existing the node's stored copy, or null when there is none
     * @return the updated node with retained datapath ids
     */
    static KubevirtNode withRetainedPhysBridgeIds(KubevirtNode updated, KubevirtNode existing) {
        if (existing == null || updated.phyIntfs().isEmpty()) {
            return updated;
        }

        Map<String, DeviceId> storedIds = new HashMap<>();
        existing.phyIntfs().stream()
                .filter(pi -> pi.physBridge() != null)
                .forEach(pi -> storedIds.putIfAbsent(pi.network(), pi.physBridge()));

        boolean changed = false;
        List<KubevirtPhyInterface> retained = new ArrayList<>();
        for (KubevirtPhyInterface pi : updated.phyIntfs()) {
            DeviceId stored = storedIds.get(pi.network());
            DeviceId generated = DeviceId.deviceId(
                    genDpidFromName(pi.network() + pi.intf() + updated.hostname()));
            if (stored == null || stored.equals(pi.physBridge()) ||
                    !generated.equals(pi.physBridge())) {
                retained.add(pi);
                continue;
            }
            retained.add(DefaultKubevirtPhyInterface.builder()
                    .network(pi.network())
                    .intf(pi.intf())
                    .kaasElbs(pi.kaasElbs())
                    .physBridge(stored)
                    .build());
            changed = true;
        }

        return changed ? DefaultKubevirtNode.from(updated).phyIntfs(retained).build() : updated;
    }

    static Set<DeviceId> conflictingPhysBridgeIds(KubevirtNode node, Set<KubevirtNode> others) {
        Set<DeviceId> seen = new HashSet<>();
        Set<DeviceId> conflicts = new HashSet<>();
        node.phyIntfs().forEach(pi -> {
            if (!seen.add(pi.physBridge())) {
                conflicts.add(pi.physBridge());
            }
        });

        others.stream()
                .filter(other -> !Objects.equals(other.hostname(), node.hostname()))
                .flatMap(other -> Stream.concat(
                        Stream.of(other.intgBridge(), other.tunBridge()),
                        other.phyIntfs().stream().map(KubevirtPhyInterface::physBridge)))
                .filter(Objects::nonNull)
                .filter(seen::contains)
                .forEach(conflicts::add);

        return conflicts;
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
            if (e != null && e.isHttpGone()) {
                // expected 410: our (stale) resourceVersion aged out of the
                // API server watch cache; the reconnect re-lists from a fresh
                // version, so log it at debug without the noisy stack trace
                log.debug("Node watcher expired (too old resource version), " +
                        "re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Node watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleReconnect();
        }

        private void processAddition(Node node) {
            if (!isMaster()) {
                return;
            }

            KubevirtNode kubevirtNode = buildKubevirtNode(node);
            log.info("buildKubevirtNode: {}", kubevirtNode);

            // an unparseable annotation leaves the intended configuration
            // unknown; nothing is running on an unregistered node yet, so
            // fail fast and make the operator fix the annotation
            if (kubevirtNode == null) {
                log.error("Refusing to register node {} whose SONA annotations " +
                        "could not be parsed", node.getMetadata().getName());
                return;
            }

            // a physnet-config annotation that maps one network to several
            // interfaces would attach every one of them to the same
            // NORMAL-forwarding physnet bridge, bridging the NICs into one
            // L2 segment and looping the physical fabric; nothing is running
            // on an unregistered node yet, so fail fast and make the operator
            // fix the annotation
            Set<String> duplicated = duplicatedPhysnetValues(
                    kubevirtNode, KubevirtPhyInterface::network);
            if (!duplicated.isEmpty()) {
                log.error("Refusing to register node {} whose physnet-config " +
                        "annotation declares more than one interface for " +
                        "network(s) {}", kubevirtNode.hostname(), duplicated);
                return;
            }

            // the mirror image, one interface declared for several networks,
            // is just as broken: a NIC can only sit in one bridge, so the
            // bridge provisioned first claims it and the other physnet is
            // left without an uplink, with the winner depending on
            // iteration order
            Set<String> shared = duplicatedPhysnetValues(
                    kubevirtNode, KubevirtPhyInterface::intf);
            if (!shared.isEmpty()) {
                log.error("Refusing to register node {} whose physnet-config " +
                        "annotation declares interface(s) {} for more than one " +
                        "network", kubevirtNode.hostname(), shared);
                return;
            }

            // a phys_bridge_id shared by two physnet bridges makes ONOS treat
            // two OVS switches as one OpenFlow device and install one bridge's
            // flows on the other host; the generated ids are a zero-padded
            // 32-bit hash, so this can also happen without an operator typo
            Set<DeviceId> conflicts = conflictingPhysBridgeIds(
                    kubevirtNode, kubevirtNodeAdminService.nodes());
            if (!conflicts.isEmpty()) {
                log.error("Refusing to register node {} whose physnet bridge " +
                        "datapath id(s) {} are declared twice or already assigned " +
                        "to a bridge of another node", kubevirtNode.hostname(), conflicts);
                return;
            }

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

            // an unparseable annotation used to build the node with whatever
            // the surviving annotations said, e.g. a gateway demoted to its
            // label-derived MASTER type, which the branches below read as
            // "gateway annotation removed" and tore the live node down;
            // ignore the event and keep the last known good configuration
            if (original == null) {
                log.warn("Ignoring update for node {} whose SONA annotations " +
                        "could not be parsed; keeping its last known configuration",
                        node.getMetadata().getName());
                return;
            }

            // keep the physnet bridge identity of a network across uplink
            // changes; done before the validations below so they judge the
            // ids the node will actually carry
            KubevirtNode retained = withRetainedPhysBridgeIds(original, existing);
            if (retained != original) {
                log.info("Keeping the stored physnet bridge datapath ids of node {} " +
                        "across its physnet-config update", original.hostname());
                original = retained;
            }

            // same duplicate-network hazard as in processAddition, but here
            // the node may already be serving VMs, so acting on the broken
            // annotation in any way (re-INIT, gateway add/remove) risks a
            // data-plane outage; ignore the event entirely and keep the
            // node's last known good configuration until the operator fixes
            // the annotation
            Set<String> duplicated = duplicatedPhysnetValues(
                    original, KubevirtPhyInterface::network);
            if (!duplicated.isEmpty()) {
                log.warn("Ignoring update for node {} whose physnet-config " +
                        "annotation declares more than one interface for " +
                        "network(s) {}; keeping its last known configuration",
                        original.hostname(), duplicated);
                return;
            }

            // same shared-interface hazard as in processAddition
            Set<String> shared = duplicatedPhysnetValues(
                    original, KubevirtPhyInterface::intf);
            if (!shared.isEmpty()) {
                log.warn("Ignoring update for node {} whose physnet-config " +
                        "annotation declares interface(s) {} for more than one " +
                        "network; keeping its last known configuration",
                        original.hostname(), shared);
                return;
            }

            // same datapath id hazard as in processAddition; the node's own
            // stored copy is excluded from the comparison since the update
            // replaces it
            Set<DeviceId> conflicts = conflictingPhysBridgeIds(
                    original, kubevirtNodeAdminService.nodes());
            if (!conflicts.isEmpty()) {
                log.warn("Ignoring update for node {} whose physnet bridge " +
                        "datapath id(s) {} are declared twice or already assigned " +
                        "to a bridge of another node; keeping its last known " +
                        "configuration", original.hostname(), conflicts);
                return;
            }

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
                //
                // phyIntfs() may be a HashSet on the annotation-derived side and an
                // ArrayList on the REST/codec-decoded side, so compare by content;
                // a raw Collection.equals() is List-vs-Set unequal and would trigger
                // a spurious re-INIT on every heartbeat. dataIp may be null on a
                // REST-only node, so compare null-safely.
                boolean phyIntfsChanged = !ImmutableSet.copyOf(original.phyIntfs())
                        .equals(ImmutableSet.copyOf(existing.phyIntfs()));
                boolean dataIpChanged = !Objects.equals(original.dataIp(), existing.dataIp());

                // a MODIFIED event whose node object carries no physnet annotation
                // yields an empty phyIntfs; never let that wipe a node that already
                // has physical interfaces, since the forced INIT tears down its
                // physnet bridges and causes a data-plane outage
                if (phyIntfsChanged && original.phyIntfs().isEmpty()
                        && !existing.phyIntfs().isEmpty()) {
                    log.warn("Skipping update for node {} that would clear its " +
                            "physical interfaces; the API node object carries no " +
                            "physnet annotation", original.hostname());
                    return;
                }

                if (phyIntfsChanged || dataIpChanged) {
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
