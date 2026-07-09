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

import com.google.common.collect.ImmutableSet;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.util.KryoNamespace;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.kubevirtnode.api.DefaultKubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigStore;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigStoreDelegate;
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
import static org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent.Type.KUBERNETES_EXTERNAL_LB_CONFIG_CREATED;
import static org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent.Type.KUBERNETES_EXTERNAL_LB_CONFIG_REMOVED;
import static org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigEvent.Type.KUBERNETES_EXTERNAL_LB_CONFIG_UPDATED;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of kubernetes external lb config store using consistent map.
 */
@Component(immediate = true, service = KubernetesExternalLbConfigStore.class)
public class DistributedKubernetesExternalLbConfigStore
    extends AbstractStore<KubernetesExternalLbConfigEvent, KubernetesExternalLbConfigStoreDelegate>
    implements KubernetesExternalLbConfigStore {

    private final Logger log = getLogger(getClass());

    private static final String ERR_NOT_FOUND = " does not exist";
    private static final String ERR_DUPLICATE = " already exists";
    private static final String APP_ID = "org.onosproject.kubevirtnode";

    private static final KryoNamespace
            SERIALIZER_KUBERNETES_EXTERNAL_LB_CONFIG = KryoNamespace.newBuilder()
            .register(KryoNamespaces.API)
            .register(KubernetesExternalLbConfig.class)
            .register(DefaultKubernetesExternalLbConfig.class)
            .register(IpAddress.class)
            .register(MacAddress.class)
            .register(Collection.class)
            .build();

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected StorageService storageService;

    private final ExecutorService eventExecutor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "event-handler", log));

    private final MapEventListener<String, KubernetesExternalLbConfig> lbConfigMapEventListener =
            new KubernetesExternalLbConfigMapListener();

    private ConsistentMap<String, KubernetesExternalLbConfig> lbConfigStore;

    @Activate
    protected void activate() {
        ApplicationId appId = coreService.registerApplication(APP_ID);
        lbConfigStore = storageService.<String, KubernetesExternalLbConfig>consistentMapBuilder()
                .withSerializer(Serializer.using(SERIALIZER_KUBERNETES_EXTERNAL_LB_CONFIG))
                .withName("kubernetes-lbconfigstore")
                .withApplicationId(appId)
                .build();

        lbConfigStore.addListener(lbConfigMapEventListener);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        lbConfigStore.removeListener(lbConfigMapEventListener);
        eventExecutor.shutdown();
        log.info("Stopped");
    }

    @Override
    public void createExternalLbConfig(KubernetesExternalLbConfig lbConfig) {
        Versioned<KubernetesExternalLbConfig> existing =
                lbConfigStore.putIfAbsent(lbConfig.configName(), lbConfig);
        if (existing != null && !lbConfig.equals(existing.value())) {
            throw new IllegalArgumentException(lbConfig.configName() + ERR_DUPLICATE);
        }
    }

    @Override
    public void updateExternalLbConfig(KubernetesExternalLbConfig lbConfig) {
        Versioned<KubernetesExternalLbConfig> result =
                lbConfigStore.computeIfPresent(lbConfig.configName(), (configName, existing) -> {
                    if (lbConfig.equals(existing) && lbConfig.loadBalancerGwMac() == null &&
                            existing.loadBalancerGwMac() != null) {
                        return existing;
                    } else {
                        return lbConfig;
                    }
                });
        if (result == null) {
            throw new IllegalArgumentException(lbConfig.configName() + ERR_NOT_FOUND);
        }
    }

    @Override
    public KubernetesExternalLbConfig removeExternalLbConfig(String configName) {

        Versioned<KubernetesExternalLbConfig> lbConfig = lbConfigStore.remove(configName);

        if (lbConfig == null) {
            final String error = configName + ERR_NOT_FOUND;
            throw new IllegalArgumentException(error);
        }

        return lbConfig.value();
    }

    @Override
    public KubernetesExternalLbConfig externalLbConfig(String configName) {
        return lbConfigStore.asJavaMap().get(configName);
    }

    @Override
    public Set<KubernetesExternalLbConfig> externalLbConfigs() {

        return ImmutableSet.copyOf(lbConfigStore.asJavaMap().values());
    }

    @Override
    public void clear() {
        lbConfigStore.clear();
    }

    private class KubernetesExternalLbConfigMapListener
            implements MapEventListener<String, KubernetesExternalLbConfig> {

        @Override
        public void event(MapEvent<String, KubernetesExternalLbConfig> event) {
            switch (event.type()) {
                case INSERT:
                    eventExecutor.execute(() ->
                            notifyDelegate(new KubernetesExternalLbConfigEvent(
                                    KUBERNETES_EXTERNAL_LB_CONFIG_CREATED, event.newValue().value())));
                    break;
                case UPDATE:
                    eventExecutor.execute(() ->
                            notifyDelegate(new KubernetesExternalLbConfigEvent(
                                    KUBERNETES_EXTERNAL_LB_CONFIG_UPDATED, event.newValue().value())));
                    break;
                case REMOVE:
                    eventExecutor.execute(() ->
                            notifyDelegate(new KubernetesExternalLbConfigEvent(
                                    KUBERNETES_EXTERNAL_LB_CONFIG_REMOVED, event.oldValue().value())));
                    break;
                default:
                    //do nothing
                    break;
            }
        }
    }
}
