/*
 * Copyright 2026-present Open Networking Foundation
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
import org.junit.Test;
import org.onlab.packet.IpAddress;
import org.onosproject.kubevirtnode.api.DefaultKubevirtNode;
import org.onosproject.kubevirtnode.api.DefaultKubevirtPhyInterface;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtNodeState;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;
import org.onosproject.net.DeviceId;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.WORKER;
import static org.onosproject.kubevirtnode.impl.KubevirtNodeWatcher.conflictingPhysBridgeIds;
import static org.onosproject.kubevirtnode.impl.KubevirtNodeWatcher.duplicatedPhysnetValues;
import static org.onosproject.kubevirtnode.impl.KubevirtNodeWatcher.withRetainedPhysBridgeIds;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.genDpidFromName;

/**
 * Unit tests for the annotation validation helpers of the node watcher.
 */
public class KubevirtNodeWatcherTest {

    private static final DeviceId DPID_1 = DeviceId.deviceId("of:0000000000000001");
    private static final DeviceId DPID_2 = DeviceId.deviceId("of:0000000000000002");
    private static final DeviceId DPID_3 = DeviceId.deviceId("of:0000000000000003");
    private static final DeviceId DPID_4 = DeviceId.deviceId("of:0000000000000004");

    /**
     * Checks that distinct datapath ids, within the node and across nodes,
     * raise no conflict.
     */
    @Test
    public void testDistinctPhysBridgeIds() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth2", DPID_2));
        KubevirtNode other = node("worker-2", DPID_3, DPID_4,
                phyIntf("net1", "eth1", DPID_3));

        assertTrue(conflictingPhysBridgeIds(node, ImmutableSet.of(other)).isEmpty());
    }

    /**
     * Checks that a datapath id declared for two networks of the same node
     * is reported.
     */
    @Test
    public void testPhysBridgeIdDeclaredTwice() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth2", DPID_1));

        assertEquals(ImmutableSet.of(DPID_1),
                conflictingPhysBridgeIds(node, ImmutableSet.of()));
    }

    /**
     * Checks that a datapath id already assigned to a physnet bridge of
     * another node is reported.
     */
    @Test
    public void testPhysBridgeIdReusedByOtherNode() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth2", DPID_2));
        KubevirtNode other = node("worker-2", DPID_3, DPID_4,
                phyIntf("net1", "eth1", DPID_2));

        assertEquals(ImmutableSet.of(DPID_2),
                conflictingPhysBridgeIds(node, ImmutableSet.of(other)));
    }

    /**
     * Checks that a datapath id already assigned to the integration or
     * tunnel bridge of another node is reported.
     */
    @Test
    public void testPhysBridgeIdReusedByOtherNodeCoreBridge() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_3));
        KubevirtNode other = node("worker-2", DPID_3, DPID_4);

        assertEquals(ImmutableSet.of(DPID_3),
                conflictingPhysBridgeIds(node, ImmutableSet.of(other)));
    }

    /**
     * Checks that the node's own stored copy, which an update replaces, is
     * not compared against the update.
     */
    @Test
    public void testOwnStoredCopyIgnored() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1));
        KubevirtNode stored = node("worker-1", DPID_3, DPID_4,
                phyIntf("net1", "eth1", DPID_1));

        assertTrue(conflictingPhysBridgeIds(node, ImmutableSet.of(stored)).isEmpty());
    }

    /**
     * Checks that distinct networks and interfaces raise no duplicate.
     */
    @Test
    public void testDistinctNetworksAndInterfaces() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth2", DPID_2));

        assertTrue(duplicatedPhysnetValues(node, KubevirtPhyInterface::network).isEmpty());
        assertTrue(duplicatedPhysnetValues(node, KubevirtPhyInterface::intf).isEmpty());
    }

    /**
     * Checks that a network declared with two interfaces is reported.
     */
    @Test
    public void testNetworkDeclaredTwice() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net1", "eth2", DPID_2));

        assertEquals(ImmutableSet.of("net1"),
                duplicatedPhysnetValues(node, KubevirtPhyInterface::network));
        assertTrue(duplicatedPhysnetValues(node, KubevirtPhyInterface::intf).isEmpty());
    }

    /**
     * Checks that an interface declared for two networks is reported.
     */
    @Test
    public void testInterfaceDeclaredForTwoNetworks() {
        KubevirtNode node = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth1", DPID_2));

        assertTrue(duplicatedPhysnetValues(node, KubevirtPhyInterface::network).isEmpty());
        assertEquals(ImmutableSet.of("eth1"),
                duplicatedPhysnetValues(node, KubevirtPhyInterface::intf));
    }

    /**
     * Checks that a network re-pointed at another interface without an
     * explicit datapath id keeps the id its bridge already has.
     */
    @Test
    public void testGeneratedPhysBridgeIdRetainedAcrossUplinkChange() {
        DeviceId oldId = generatedId("net1", "eth1", "worker-1");
        DeviceId newId = generatedId("net1", "eth2", "worker-1");
        KubevirtNode existing = node("worker-1", null, null, phyIntf("net1", "eth1", oldId));
        KubevirtNode updated = node("worker-1", null, null, phyIntf("net1", "eth2", newId));

        KubevirtNode retained = withRetainedPhysBridgeIds(updated, existing);

        KubevirtPhyInterface pi = retained.phyIntfs().iterator().next();
        assertEquals("eth2", pi.intf());
        assertEquals(oldId, pi.physBridge());
    }

    /**
     * Checks that an explicitly declared datapath id is taken as is even
     * when it differs from the stored one.
     */
    @Test
    public void testExplicitPhysBridgeIdNotRetained() {
        KubevirtNode existing = node("worker-1", null, null, phyIntf("net1", "eth1", DPID_1));
        KubevirtNode updated = node("worker-1", null, null, phyIntf("net1", "eth2", DPID_2));

        KubevirtNode retained = withRetainedPhysBridgeIds(updated, existing);

        assertEquals(DPID_2, retained.phyIntfs().iterator().next().physBridge());
    }

    /**
     * Checks that a network the stored copy does not know, and a node with
     * no stored copy at all, keep their generated ids.
     */
    @Test
    public void testNothingToRetain() {
        DeviceId generated = generatedId("net2", "eth2", "worker-1");
        KubevirtNode existing = node("worker-1", null, null, phyIntf("net1", "eth1", DPID_1));
        KubevirtNode updated = node("worker-1", null, null,
                phyIntf("net1", "eth1", DPID_1), phyIntf("net2", "eth2", generated));

        assertEquals(updated, withRetainedPhysBridgeIds(updated, existing));
        assertEquals(updated, withRetainedPhysBridgeIds(updated, null));
    }

    private static DeviceId generatedId(String network, String intf, String hostname) {
        return DeviceId.deviceId(genDpidFromName(network + intf + hostname));
    }

    private static KubevirtPhyInterface phyIntf(String network, String intf, DeviceId dpid) {
        return DefaultKubevirtPhyInterface.builder()
                .network(network)
                .intf(intf)
                .physBridge(dpid)
                .build();
    }

    private static KubevirtNode node(String hostname, DeviceId intgBridge, DeviceId tunBridge,
                                     KubevirtPhyInterface... phyIntfs) {
        Set<KubevirtPhyInterface> intfs = ImmutableSet.copyOf(phyIntfs);
        return DefaultKubevirtNode.builder()
                .hostname(hostname)
                .type(WORKER)
                .state(KubevirtNodeState.INIT)
                .managementIp(IpAddress.valueOf("10.10.0.1"))
                .intgBridge(intgBridge)
                .tunBridge(tunBridge)
                .phyIntfs(intfs)
                .build();
    }
}
