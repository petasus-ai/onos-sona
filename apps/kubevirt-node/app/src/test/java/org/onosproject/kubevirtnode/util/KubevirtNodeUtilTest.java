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
package org.onosproject.kubevirtnode.util;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import org.junit.Test;
import org.onlab.packet.IpAddress;
import org.onosproject.kubevirtnode.api.KubevirtNode;
import org.onosproject.kubevirtnode.api.KubevirtPhyInterface;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.onosproject.kubevirtnode.api.Constants.SONA_PROJECT_DOMAIN;
import static org.onosproject.kubevirtnode.api.KubevirtNode.Type.GATEWAY;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.buildKubevirtNode;

/**
 * Unit tests for the node annotation parsing of KubevirtNodeUtil.
 */
public class KubevirtNodeUtilTest {

    private static final String HOSTNAME = "gw-1";
    private static final String INTERNAL_IP = "10.10.0.1";
    private static final String DATA_IP = "10.20.0.1";

    private static final String PHYSNET_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/physnet-config";
    private static final String TUNNEL_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/tunnel-config";
    private static final String GATEWAY_CONFIG_KEY = SONA_PROJECT_DOMAIN + "/gateway-config";

    private static final String PHYSNET_CONFIG =
            "[{\"network\": \"physnet1\", \"interface\": \"eth1\"}]";
    private static final String TUNNEL_CONFIG = "{\"ip\": \"" + DATA_IP + "\"}";
    private static final String GATEWAY_CONFIG = "{\"gatewayBridgeName\": \"br-gateway\"}";

    private static final Map<String, String> MASTER_LABELS =
            ImmutableMap.of("node-role.kubernetes.io/control-plane", "");

    /**
     * Checks that a node whose annotations are all well-formed builds as a
     * gateway with the declared physnet and tunnel configuration.
     */
    @Test
    public void testBuildGatewayNode() {
        KubevirtNode node = buildKubevirtNode(node(
                PHYSNET_CONFIG, TUNNEL_CONFIG, GATEWAY_CONFIG));

        assertNotNull(node);
        assertEquals(HOSTNAME, node.hostname());
        assertEquals(GATEWAY, node.type());
        assertEquals(IpAddress.valueOf(INTERNAL_IP), node.managementIp());
        assertEquals(IpAddress.valueOf(DATA_IP), node.dataIp());
        assertEquals("br-gateway", node.gatewayBridgeName());
        assertEquals(1, node.phyIntfs().size());

        KubevirtPhyInterface pi = node.phyIntfs().iterator().next();
        assertEquals("physnet1", pi.network());
        assertEquals("eth1", pi.intf());
        assertNotNull(pi.physBridge());
    }

    /**
     * Checks that a syntax error in tunnel-config does not let the node
     * build with its label-derived type; the build must fail instead of
     * demoting the gateway to a master.
     */
    @Test
    public void testMalformedTunnelConfigFailsBuild() {
        assertNull(buildKubevirtNode(node(
                PHYSNET_CONFIG, "{\"ip\": \"" + DATA_IP + "\"", GATEWAY_CONFIG)));
    }

    /**
     * Checks that a tunnel-config without the ip key fails the build.
     */
    @Test
    public void testTunnelConfigWithoutIpFailsBuild() {
        assertNull(buildKubevirtNode(node(
                PHYSNET_CONFIG, "{\"address\": \"" + DATA_IP + "\"}", GATEWAY_CONFIG)));
    }

    /**
     * Checks that a syntax error in gateway-config fails the build.
     */
    @Test
    public void testMalformedGatewayConfigFailsBuild() {
        assertNull(buildKubevirtNode(node(
                PHYSNET_CONFIG, TUNNEL_CONFIG, "{\"gatewayBridgeName\": br-gateway}")));
    }

    /**
     * Checks that a syntax error in physnet-config fails the build instead
     * of escaping as an unchecked exception.
     */
    @Test
    public void testMalformedPhysnetConfigFailsBuild() {
        assertNull(buildKubevirtNode(node(
                "[{\"network\": \"physnet1\", \"interface\": \"eth1\"", TUNNEL_CONFIG, GATEWAY_CONFIG)));
    }

    /**
     * Checks that a physnet-config entry missing the interface key fails
     * the build instead of escaping as an unchecked exception.
     */
    @Test
    public void testPhysnetEntryWithoutInterfaceFailsBuild() {
        assertNull(buildKubevirtNode(node(
                "[{\"network\": \"physnet1\"}]", TUNNEL_CONFIG, GATEWAY_CONFIG)));
    }

    /**
     * Checks that a physnet-config whose entry is not an object fails the
     * build instead of escaping as an unchecked exception.
     */
    @Test
    public void testPhysnetNonObjectEntryFailsBuild() {
        assertNull(buildKubevirtNode(node(
                "[\"physnet1\"]", TUNNEL_CONFIG, GATEWAY_CONFIG)));
    }

    private static Node node(String physnetConfig, String tunnelConfig, String gatewayConfig) {
        Map<String, String> annots = new HashMap<>();
        if (physnetConfig != null) {
            annots.put(PHYSNET_CONFIG_KEY, physnetConfig);
        }
        if (tunnelConfig != null) {
            annots.put(TUNNEL_CONFIG_KEY, tunnelConfig);
        }
        if (gatewayConfig != null) {
            annots.put(GATEWAY_CONFIG_KEY, gatewayConfig);
        }

        return new NodeBuilder()
                .withNewMetadata()
                    .withName(HOSTNAME)
                    .withLabels(MASTER_LABELS)
                    .withAnnotations(annots)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .addNewAddress()
                        .withType("InternalIP")
                        .withAddress(INTERNAL_IP)
                    .endAddress()
                .endStatus()
                .build();
    }
}
