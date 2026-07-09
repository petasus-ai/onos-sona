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
package org.onosproject.kubevirtnetworking.impl;

import com.google.common.collect.ImmutableSet;
import org.onlab.packet.IpAddress;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnetworking.api.DefaultKubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.DefaultKubernetesServicePort;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLbStore;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLbStoreDelegate;
import org.onosproject.kubevirtnetworking.api.KubernetesServicePort;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.ConsistentMap;
import org.onosproject.store.service.MapEvent;
import org.onosproject.store.service.MapEventListener;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.Versioned;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent.Type.KUBERNETES_EXTERNAL_LOAD_BALANCER_CREATED;
import static org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent.Type.KUBERNETES_EXTERNAL_LOAD_BALANCER_GATEWAY_CHANGED;
import static org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent.Type.KUBERNETES_EXTERNAL_LOAD_BALANCER_REMOVED;
import static org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent.Type.KUBERNETES_EXTERNAL_LOAD_BALANCER_UPDATED;
import static org.onosproject.kubevirtnetworking.api.KubernetesExternalLbEvent.Type.KUBERNETES_EXTERNAL_LOAD_BALANCER_WORKER_CHANGED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of kubernetes external load balancer store using consistent map.
 */
@Component(immediate = true, service = KubernetesExternalLbStore.class)
public class DistributedKubernetesExternalLbStore
    extends AbstractStore<KubernetesExternalLbEvent, KubernetesExternalLbStoreDelegate>
    implements KubernetesExternalLbStore {

    private final Logger log = getLogger(getClass());

    private static final String ERR_NOT_FOUND = " does not exist";
    private static final String ERR_DUPLICATE = " already exists";
    private static final String APP_ID = "org.onosproject.kubevirtnetwork";

    private static final KryoNamespace SERIALIZER_KUBERNETES_EXTERNAL_LB = KryoNamespace.newBuilder()
            .register(KryoNamespaces.API)
            .register(KubernetesExternalLb.class)
            .register(DefaultKubernetesExternalLb.class)
            .register(KubernetesServicePort.class)
            .register(DefaultKubernetesServicePort.class)
            .register(IpAddress.class)
            .register(Collection.class)
            .build();

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected StorageService storageService;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));

    private final MapEventListener<String, KubernetesExternalLb> lbMapEventListener =
            new KubernetesExternalLbMapListener();

    private ConsistentMap<String, KubernetesExternalLb> lbStore;

    @Activate
    protected void activate() {
        ApplicationId appId = coreService.registerApplication(APP_ID);
        lbStore = storageService.<String, KubernetesExternalLb>consistentMapBuilder()
                .withSerializer(Serializer.using(SERIALIZER_KUBERNETES_EXTERNAL_LB))
                .withName("kubernetes-lbstore")
                .withApplicationId(appId)
                .build();

        lbStore.addListener(lbMapEventListener);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        lbStore.removeListener(lbMapEventListener);
        eventExecutor.shutdown();
        log.info("Stopped");
    }

    @Override
    public void createLoadBalancer(KubernetesExternalLb lb) {
        Versioned<KubernetesExternalLb> existing = lbStore.putIfAbsent(lb.serviceName(), lb);
        if (existing != null && !lb.equals(existing.value())) {
            throw new IllegalArgumentException(lb.serviceName() + ERR_DUPLICATE);
        }
    }

    @Override
    public void updateLoadBalancer(KubernetesExternalLb lb) {
        Versioned<KubernetesExternalLb> result =
                lbStore.computeIfPresent(lb.serviceName(), (seviceName, existing) -> lb);
        if (result == null) {
            throw new IllegalArgumentException(lb.serviceName() + ERR_NOT_FOUND);
        }
    }

    @Override
    public KubernetesExternalLb removeLoadBalancer(String serviceName) {

        Versioned<KubernetesExternalLb> lb = lbStore.remove(serviceName);

        if (lb == null) {
            final String error = serviceName + ERR_NOT_FOUND;
            throw new IllegalArgumentException(error);
        }
        return lb.value();
    }

    @Override
    public KubernetesExternalLb loadBalancer(String serviceName) {
        return lbStore.asJavaMap().get(serviceName);
    }

    @Override
    public Set<KubernetesExternalLb> loadBalancers() {
        return  ImmutableSet.copyOf(lbStore.asJavaMap().values());
    }

    @Override
    public void clear() {
        lbStore.clear();
    }

    private class KubernetesExternalLbMapListener implements MapEventListener<String, KubernetesExternalLb> {
        @Override
        public void event(MapEvent<String, KubernetesExternalLb> event) {
            switch (event.type()) {
                case INSERT:
                    eventExecutor.execute(() ->
                            notifyDelegate(new KubernetesExternalLbEvent(
                                    KUBERNETES_EXTERNAL_LOAD_BALANCER_CREATED, event.newValue().value())));
                    break;
                case UPDATE:
                    eventExecutor.execute(() -> processMapUpdate(event));
                    break;
                case REMOVE:
                    eventExecutor.execute(() ->
                            notifyDelegate(new KubernetesExternalLbEvent(
                                    KUBERNETES_EXTERNAL_LOAD_BALANCER_REMOVED, event.oldValue().value())));
                    break;
                default:
                    //do nothing
                    break;
            }
        }

        private void processMapUpdate(MapEvent<String, KubernetesExternalLb> event) {
            log.debug("Kubernetes External LB updated");

            KubernetesExternalLb oldValue = event.oldValue().value();
            KubernetesExternalLb newValue = event.newValue().value();

            if (oldValue.electedGateway() != null && newValue.electedGateway() != null &&
                    !oldValue.electedGateway().equals(newValue.electedGateway())) {
                notifyDelegate(new KubernetesExternalLbEvent(
                        KUBERNETES_EXTERNAL_LOAD_BALANCER_GATEWAY_CHANGED,
                        newValue, oldValue.electedGateway(), oldValue.electedWorker())
                );
            }

            if (oldValue.electedWorker() != null && newValue.electedWorker() != null &&
                    !oldValue.electedWorker().equals(newValue.electedWorker())) {
                notifyDelegate(new KubernetesExternalLbEvent(
                        KUBERNETES_EXTERNAL_LOAD_BALANCER_WORKER_CHANGED,
                        newValue, oldValue.electedWorker())
                );
            }

            notifyDelegate(new KubernetesExternalLbEvent(
                    KUBERNETES_EXTERNAL_LOAD_BALANCER_UPDATED, event.newValue().value()));

        }
    }
}
