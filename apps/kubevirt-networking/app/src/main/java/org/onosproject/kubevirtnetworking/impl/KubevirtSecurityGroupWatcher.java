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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.Constants.KUBEVIRT_NETWORKING_APP_ID;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.k8sClient;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.waitFor;
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

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));
    private final ScheduledExecutorService reconnectExecutor = newSingleThreadScheduledExecutor(
            groupedThreads(this.getClass().getSimpleName(), "watch-reconnect", log));

    // the clients owning the currently active watches; closing them terminates
    // the watches, so keeping exactly one instance per watch prevents both
    // duplicated watches and client (thread/connection pool) leaks
    private KubernetesClient sgWatchClient;
    private KubernetesClient sgrWatchClient;

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
        sgWatchClient = k8sClient(configService);

        if (sgWatchClient == null) {
            scheduleSgReconnect();
            return;
        }

        try {
            sgWatch = sgWatchClient.customResource(securityGroupCrdCxt).watch(sgWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate security group watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgReconnect();
        }
    }

    private synchronized void instantiateSgrWatcher() {
        closeSgrWatch();
        closeSgrWatchClient();
        sgrWatchClient = k8sClient(configService);

        if (sgrWatchClient == null) {
            scheduleSgrReconnect();
            return;
        }

        try {
            sgrWatch = sgrWatchClient.customResource(securityGroupRuleCrdCxt).watch(sgrWatcher);
        } catch (Exception e) {
            log.error("Failed to instantiate security group rule watcher, retrying in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgrReconnect();
        }
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
            log.warn("Security group watcher closed, re-instantiating in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgReconnect();
        }

        private void processAddition(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroup sg = parseSecurityGroup(resource);

            if (sg != null) {
                log.trace("Process Security Group {} creating event from API server.", sg.name());

                if (adminService.securityGroup(sg.id()) == null) {
                    adminService.createSecurityGroup(sg);
                }
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
                // we need to manually add all rules from original to the updated one
                KubevirtSecurityGroup orig = adminService.securityGroup(sg.id());
                if (orig != null) {
                    KubevirtSecurityGroup updated = sg.updateRules(orig.rules());
                    adminService.updateSecurityGroup(updated);
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
            log.warn("Security group rule watcher closed, re-instantiating in {}s",
                    RECONNECT_DELAY_S, e);
            scheduleSgrReconnect();
        }

        private void processAddition(String resource) {
            if (!isMaster()) {
                return;
            }

            KubevirtSecurityGroupRule sgr = parseSecurityGroupRule(resource);

            if (sgr != null) {
                log.trace("Process Security Group Rule {} creating event from API server.", sgr.id());

                KubevirtSecurityGroup sg = adminService.securityGroup(sgr.securityGroupId());
                if (sg == null) {
                    log.warn("Security Group {} is not found, we wait 5 seconds until " +
                             "the group to be installed.", sgr.securityGroupId());
                    waitFor(5);
                }

                if (adminService.securityGroupRule(sgr.id()) == null) {
                    adminService.createSecurityGroupRule(sgr);
                }
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

                adminService.removeSecurityGroupRule(sgr.id());
            }
        }
    }

    private boolean isMaster() {
        return Objects.equals(localNodeId, leadershipService.getLeader(appId.name()));
    }
}
