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
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.onosproject.kubevirtnetworking.api.AbstractWatcher;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroup;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroupAdminService;
import org.onosproject.kubevirtnetworking.api.KubevirtSecurityGroupRule;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kubevirt security group watcher used for feeding kubevirt security group information.
 */
@Component(immediate = true)
public class KubevirtSecurityGroupWatcher extends AbstractWatcher {

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
    protected KubevirtSecurityGroupAdminService adminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected KubevirtApiConfigService configService;

    private static final long RECONNECT_DELAY_S = 5;
    private static final int ORPHAN_RULE_RETRY_LIMIT = 10;
    private static final long ORPHAN_RULE_RETRY_DELAY_S = 3;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));
    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));

    // the store resync (list + prune) makes a blocking API-server call, so it
    // runs off both the event and reconnect threads to avoid stalling event
    // processing or delaying a pending re-watch
    private final ExecutorService resyncExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "store-resync", log));

    // the clients owning the currently active watches; closing them terminates
    // the watches, so keeping exactly one instance per watch prevents both
    // duplicated watches and client (thread/connection pool) leaks
    private volatile KubernetesClient sgWatchClient;
    private volatile KubernetesClient sgrWatchClient;

    // the handles of the active watches; they MUST be closed before their
    // clients, otherwise the fabric8 watch manager keeps re-running its
    // reconnect loop on top of a terminated dispatcher, flooding the log
    // with "Exec Failure ... executor rejected" warnings
    private Watch sgWatch;
    private Watch sgrWatch;

    // set while WE close a watch on purpose (re-instantiation, shutdown);
    // an onClose callback fired by that intentional close must not schedule
    // another re-instantiation, or the watch would flap forever
    private volatile boolean closingSgWatch;
    private volatile boolean closingSgrWatch;

    // rules whose parent group has not reached the store yet, keyed by rule
    // id; groups and rules arrive on separate watches with no ordering
    // guarantee, so a rule showing up first is normal (typical on the initial
    // sync replay). Entries leave the map when the group watcher flushes
    // them, when the rule is deleted, or when the bounded retries expire.
    private final Map<String, KubevirtSecurityGroupRule> pendingRules =
            new ConcurrentHashMap<>();

    private final InternalSecurityGroupWatcher
            sgWatcher = new InternalSecurityGroupWatcher();
    private final InternalSecurityGroupRuleWatcher
            sgrWatcher = new InternalSecurityGroupRuleWatcher();
    private final InternalKubevirtApiConfigListener
            configListener = new InternalKubevirtApiConfigListener();

    CustomResourceDefinitionContext securityGroupCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("securitygroups")
            .build();

    CustomResourceDefinitionContext securityGroupRuleCrdCxt = new CustomResourceDefinitionContext
            .Builder()
            .withGroup("kubevirt.io")
            .withScope("Cluster")
            .withVersion("v1")
            .withPlural("securitygrouprules")
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
        // so establish the watches from the current config as well; every
        // instance keeps a watch and the leader check in the event handlers
        // decides who processes the events, which also covers leadership moves
        eventExecutor.execute(this::instantiateSgWatcher);
        eventExecutor.execute(this::instantiateSgrWatcher);

        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        configService.removeListener(configListener);
        leadershipService.withdraw(appId.name());
        reconnectExecutor.shutdown();
        resyncExecutor.shutdown();
        eventExecutor.shutdown();
        closeSgWatch();
        closeSgrWatch();
        closeSgWatchClient();
        closeSgrWatchClient();

        log.info("Stopped");
    }

    private synchronized void instantiateSgWatcher() {
        closeSgWatch();
        closeSgWatchClient();
        KubernetesClient client = k8sClient(configService);
        sgWatchClient = client;

        if (client == null) {
            scheduleSgReconnect();
            return;
        }

        try {
            sgWatch = client.customResource(securityGroupCrdCxt).watch(sgWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate security group watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgReconnect();
            return;
        }

        // the watch above carries no resourceVersion, so the API server replays
        // every current security group as ADDED (processed as a rule-preserving
        // upsert); it never reports groups deleted while the watch was down, so
        // reconcile the store against a fresh listing to prune those strays
        resyncExecutor.execute(() -> resyncSecurityGroups(client));
    }

    private synchronized void instantiateSgrWatcher() {
        closeSgrWatch();
        closeSgrWatchClient();
        KubernetesClient client = k8sClient(configService);
        sgrWatchClient = client;

        if (client == null) {
            scheduleSgrReconnect();
            return;
        }

        try {
            sgrWatch = client.customResource(securityGroupRuleCrdCxt).watch(sgrWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate security group rule watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgrReconnect();
            return;
        }

        // prune security group rules deleted while the watch was down; a stale
        // rule kept in the store is an ACL that keeps being enforced after the
        // operator removed it
        resyncExecutor.execute(() -> resyncSecurityGroupRules(client));
    }

    private synchronized void closeSgWatch() {
        if (sgWatch != null) {
            try {
                closingSgWatch = true;
                sgWatch.close();
            } catch (Exception e) {
                log.debug("Failed to close the previous watch", e);
            } finally {
                closingSgWatch = false;
            }
            sgWatch = null;
        }
    }

    private synchronized void closeSgrWatch() {
        if (sgrWatch != null) {
            try {
                closingSgrWatch = true;
                sgrWatch.close();
            } catch (Exception e) {
                log.debug("Failed to close the previous watch", e);
            } finally {
                closingSgrWatch = false;
            }
            sgrWatch = null;
        }
    }

    private synchronized void closeSgWatchClient() {
        if (sgWatchClient != null) {
            sgWatchClient.close();
            sgWatchClient = null;
        }
    }

    private synchronized void closeSgrWatchClient() {
        if (sgrWatchClient != null) {
            sgrWatchClient.close();
            sgrWatchClient = null;
        }
    }

    private void scheduleSgReconnect() {
        reconnectExecutor.schedule(this::instantiateSgWatcher,
                RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    private void scheduleSgrReconnect() {
        reconnectExecutor.schedule(this::instantiateSgrWatcher,
                RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    /**
     * Reconciles the security group store against the API server, removing
     * groups deleted while the watch was down. Runs only on the leader and only
     * when the full listing parses cleanly, so a partial view can never delete
     * live state.
     *
     * @param client the client whose watch triggered this resync
     */
    private void resyncSecurityGroups(KubernetesClient client) {
        if (!isMaster()) {
            return;
        }

        // a newer (re)connect may have already superseded and closed this
        // client; listing on a closed client just throws "Canceled". The
        // replacement connection queues its own resync, so skip rather than
        // log a spurious failure.
        if (client != sgWatchClient) {
            return;
        }

        // snapshot the store keys before listing: a group created between the
        // snapshot and the list carries a still-in-flight ADDED and must not be
        // mistaken for a stray
        Set<String> storedIds = adminService.securityGroups().stream()
                .map(KubevirtSecurityGroup::id)
                .collect(Collectors.toSet());

        Set<String> liveIds = liveResourceKeys(client, securityGroupCrdCxt, resource -> {
            KubevirtSecurityGroup sg = parseSecurityGroup(resource);
            return sg == null ? null : sg.id();
        });

        if (liveIds == null) {
            log.debug("Skipping security group resync: incomplete listing");
            return;
        }

        storedIds.stream()
                .filter(id -> !liveIds.contains(id))
                .forEach(id -> {
                    log.info("Pruning stale security group {} absent from API server", id);
                    try {
                        adminService.removeSecurityGroup(id);
                    } catch (Exception e) {
                        log.warn("Failed to prune stale security group {}", id, e);
                    }
                });
    }

    /**
     * Reconciles the security group rules against the API server, removing
     * rules deleted while the watch was down (a stale rule is an ACL that keeps
     * being enforced). Rules live embedded in their parent group, so the store
     * keys are gathered by flattening every group's rule set. Runs only on the
     * leader and only when the full listing parses cleanly.
     *
     * @param client the client whose watch triggered this resync
     */
    private void resyncSecurityGroupRules(KubernetesClient client) {
        if (!isMaster()) {
            return;
        }

        // a newer (re)connect may have already superseded and closed this
        // client; listing on a closed client just throws "Canceled". The
        // replacement connection queues its own resync, so skip rather than
        // log a spurious failure.
        if (client != sgrWatchClient) {
            return;
        }

        Set<String> storedIds = adminService.securityGroups().stream()
                .flatMap(sg -> sg.rules().stream())
                .map(KubevirtSecurityGroupRule::id)
                .collect(Collectors.toSet());

        Set<String> liveIds = liveResourceKeys(client, securityGroupRuleCrdCxt, resource -> {
            KubevirtSecurityGroupRule sgr = parseSecurityGroupRule(resource);
            return sgr == null ? null : sgr.id();
        });

        if (liveIds == null) {
            log.debug("Skipping security group rule resync: incomplete listing");
            return;
        }

        storedIds.stream()
                .filter(id -> !liveIds.contains(id))
                .forEach(id -> {
                    log.info("Pruning stale security group rule {} absent from API server", id);
                    try {
                        adminService.removeSecurityGroupRule(id);
                    } catch (Exception e) {
                        log.warn("Failed to prune stale security group rule {}", id, e);
                    }
                });
    }

    private KubevirtSecurityGroup parseSecurityGroup(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(resource);
            ObjectNode spec = (ObjectNode) json.get("spec");
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
            ObjectNode spec = (ObjectNode) json.get("spec");
            return codec(KubevirtSecurityGroupRule.class).decode(spec, this);
        } catch (IOException e) {
            log.error("Failed to parse kubevirt security group rule object");
        }

        return null;
    }

    private void createOrParkSecurityGroupRule(KubevirtSecurityGroupRule sgr) {
        createOrParkSecurityGroupRule(sgr, 0);
    }

    /**
     * Creates the rule if its parent group is in the store; otherwise parks it
     * until the group watcher flushes it. A bounded retry backs the flush up
     * for groups that arrive through other paths (REST, sync CLI). Never sleep
     * here instead: this runs on the shared event executor, so a sleep also
     * stalls the very group ADDED event it would be waiting for, and the
     * manager then throws IllegalStateException which kills the runnable and
     * loses the rule until the next watch reconnect.
     *
     * @param sgr     security group rule to create
     * @param attempt number of creation attempts made so far
     */
    private void createOrParkSecurityGroupRule(KubevirtSecurityGroupRule sgr, int attempt) {
        if (!isMaster()) {
            // leadership moved while the rule was parked; the new leader's
            // own watch replay covers it
            pendingRules.remove(sgr.id());
            return;
        }

        if (adminService.securityGroupRule(sgr.id()) != null) {
            pendingRules.remove(sgr.id());
            return;
        }

        if (adminService.securityGroup(sgr.securityGroupId()) != null) {
            pendingRules.remove(sgr.id());
            try {
                adminService.createSecurityGroupRule(sgr);
                return;
            } catch (IllegalStateException e) {
                // the store resync may prune the group from another thread
                // between the check and the create; park and retry below
                log.debug("Creating security group rule {} failed: {}",
                        sgr.id(), e.getMessage());
            }
        }

        if (attempt >= ORPHAN_RULE_RETRY_LIMIT) {
            pendingRules.remove(sgr.id());
            log.warn("Giving up creating security group rule {} after {} " +
                    "attempts: security group {} never arrived",
                    sgr.id(), attempt, sgr.securityGroupId());
            return;
        }

        pendingRules.put(sgr.id(), sgr);
        log.debug("Parking security group rule {} until security group {} " +
                "arrives (attempt {})", sgr.id(), sgr.securityGroupId(), attempt);
        reconnectExecutor.schedule(() -> eventExecutor.execute(() -> {
            KubevirtSecurityGroupRule pending = pendingRules.get(sgr.id());
            if (pending == null) {
                // flushed by the group watcher or deleted in the meantime
                return;
            }
            createOrParkSecurityGroupRule(pending, attempt + 1);
        }), ORPHAN_RULE_RETRY_DELAY_S, TimeUnit.SECONDS);
    }

    private void flushPendingRules(String sgId) {
        // copy first: the creation mutates the map while we iterate
        List<KubevirtSecurityGroupRule> parked = pendingRules.values().stream()
                .filter(sgr -> Objects.equals(sgr.securityGroupId(), sgId))
                .collect(Collectors.toList());
        parked.forEach(this::createOrParkSecurityGroupRule);
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

            instantiateSgWatcher();
            instantiateSgrWatcher();
        }
    }

    private class InternalSecurityGroupWatcher implements Watcher<String> {

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
                default:
                    // do nothing
                    break;
            }
        }

        @Override
        public void onClose(WatcherException e) {
            if (closingSgWatch) {
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
                log.info("Security group watcher expired (too old resource " +
                        "version), re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Security group watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleSgReconnect();
        }

        private void processAddition(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroup sg = parseSecurityGroup(resource);

            if (sg != null) {
                log.trace("Process Security Group {} creating event from API server.", sg.name());

                KubevirtSecurityGroup orig = adminService.securityGroup(sg.id());
                if (orig == null) {
                    adminService.createSecurityGroup(sg);
                } else {
                    // on a resync the API server re-delivers existing groups as
                    // ADDED; upsert so a group changed while the watch was down
                    // is not kept stale. The group CRD carries no rules, so keep
                    // the ones already reconciled from the rule CRD. Skip the
                    // write when nothing changed: every reconnect replays every
                    // group, and writing identical content just churns the
                    // store and the log
                    KubevirtSecurityGroup updated = sg.updateRules(orig.rules());
                    if (!updated.equals(orig)) {
                        adminService.updateSecurityGroup(updated);
                    }
                }

                // rules that arrived ahead of this group are parked; create
                // them now that the group is in the store
                flushPendingRules(sg.id());
            }
        }

        private void processModification(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroup sg = parseSecurityGroup(resource);

            if (sg != null) {
                log.trace("Process Security Group {} updating event from API server.", sg.name());

                // since Security Group CRD does not contains any rules information,
                // we need to manually add all rules from original to the updated one.
                // MODIFIED also fires for metadata/status-only changes that leave
                // the spec untouched, so skip the write when nothing changed
                KubevirtSecurityGroup orig = adminService.securityGroup(sg.id());
                if (orig != null) {
                    KubevirtSecurityGroup updated = sg.updateRules(orig.rules());
                    if (!updated.equals(orig)) {
                        adminService.updateSecurityGroup(updated);
                    }
                }
            }
        }

        private void processDeletion(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroup sg = parseSecurityGroup(resource);

            if (sg != null) {
                log.trace("Process Security Group {} removal event from API server.", sg.name());

                adminService.removeSecurityGroup(sg.id());
            }
        }
    }

    private class InternalSecurityGroupRuleWatcher implements Watcher<String> {

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
                default:
                    // do nothing
                    break;
            }
        }

        @Override
        public void onClose(WatcherException e) {
            if (closingSgrWatch) {
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
                log.info("Security group rule watcher expired (too old resource " +
                        "version), re-instantiating in {}s", RECONNECT_DELAY_S);
            } else {
                log.warn("Security group rule watcher closed, re-instantiating in {}s",
                        RECONNECT_DELAY_S, e);
            }
            scheduleSgrReconnect();
        }

        private void processAddition(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroupRule sgr = parseSecurityGroupRule(resource);

            if (sgr != null) {
                log.trace("Process Security Group Rule {} creating event from API server.", sgr.id());

                createOrParkSecurityGroupRule(sgr);
            }
        }

        private void processModification(String resource) {
            if (!isMaster()) {
                return;
            }

            // we do not handle the update case, as we assume the security group rule
            // object is immutable
        }

        private void processDeletion(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroupRule sgr = parseSecurityGroupRule(resource);

            if (sgr != null) {
                log.trace("Process Security Group Rule {} removal event from API server.", sgr.id());

                // drop any parked copy first, or a later flush would
                // resurrect the deleted rule as a stale ACL
                pendingRules.remove(sgr.id());

                // a rule deleted while parked never reached the store, and
                // removing an absent rule throws
                if (adminService.securityGroupRule(sgr.id()) != null) {
                    adminService.removeSecurityGroupRule(sgr.id());
                }
            }
        }
    }

    private boolean isMaster() {
        return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
    }
}
