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
package org.onosproject.kubevirtnetworking.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import org.junit.Test;
import org.onlab.packet.IpAddress;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.customResourceJson;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseKubevirtNetwork;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseResourceName;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.segmentIdHex;

/**
 * Unit tests for kubevirt networking utils.
 */
public final class KubevirtNetworkingUtilTest {

    private static String nadResource(String networkConfig) {
        return "{" +
                "\"metadata\": {" +
                "  \"name\": \"vlan-test\"," +
                "  \"annotations\": {" +
                "    \"network-config\": \"" +
                networkConfig.replace("\"", "\\\"") + "\"" +
                "  }" +
                "}" +
                "}";
    }

    /**
     * Tests the segmentIdHex method.
     */
    @Test
    public void testSegmentIdHex() {
        assertEquals("000001", segmentIdHex("1"));
        assertEquals("00000a", segmentIdHex("10"));
        assertEquals("ffffff", segmentIdHex("16777215"));
    }

    /**
     * Tests parsing a VLAN network carrying every field, including the
     * physnet binding.
     */
    @Test
    public void testParseVlanNetwork() {
        String config = "{" +
                "\"networkId\": \"default/vlan-test\"," +
                "\"name\": \"vlan-test\"," +
                "\"type\": \"VLAN\"," +
                "\"segmentId\": \"100\"," +
                "\"physnetName\": \"external\"," +
                "\"mtu\": 1500," +
                "\"cidr\": \"20.20.20.0/24\"," +
                "\"gatewayIp\": \"20.20.20.1\"," +
                "\"defaultRoute\": true," +
                "\"ipPool\": {\"start\": \"20.20.20.2\", \"end\": \"20.20.20.254\"}," +
                "\"hostRoutes\": []," +
                "\"dnses\": [\"8.8.8.8\"]" +
                "}";

        KubevirtNetwork network = parseKubevirtNetwork(nadResource(config));

        assertNotNull(network);
        assertEquals("default/vlan-test", network.networkId());
        assertEquals(KubevirtNetwork.Type.VLAN, network.type());
        assertEquals(Integer.valueOf(1500), network.mtu());
        assertEquals("100", network.segmentId());
        assertEquals("external", network.physnetName());
        assertEquals(IpAddress.valueOf("20.20.20.1"), network.gatewayIp());
        assertEquals("20.20.20.0/24", network.cidr());
    }

    /**
     * Tests parsing an L2-only network: the gateway is legitimately
     * absent and must not abort the parse.
     */
    @Test
    public void testParseNetworkWithoutGateway() {
        String config = "{" +
                "\"networkId\": \"default/storage\"," +
                "\"name\": \"storage\"," +
                "\"type\": \"VLAN\"," +
                "\"segmentId\": \"211\"," +
                "\"physnetName\": \"stg-ext\"," +
                "\"mtu\": 9000," +
                "\"cidr\": \"100.102.1.0/24\"," +
                "\"defaultRoute\": false," +
                "\"ipPool\": {\"start\": \"100.102.1.100\", \"end\": \"100.102.1.199\"}" +
                "}";

        KubevirtNetwork network = parseKubevirtNetwork(nadResource(config));

        assertNotNull(network);
        assertNull(network.gatewayIp());
        assertEquals(Integer.valueOf(9000), network.mtu());
        assertEquals("stg-ext", network.physnetName());
    }

    private static GenericKubernetesResource genericResource(String json) {
        return new KubernetesSerialization().unmarshal(json, GenericKubernetesResource.class);
    }

    /**
     * Tests that a network attachment definition delivered as a generic
     * resource parses exactly like the raw JSON string fabric8 5.x handed to
     * the watcher.
     */
    @Test
    public void testParseNetworkFromGenericResource() {
        String config = "{" +
                "\"networkId\": \"default/vlan-test\"," +
                "\"name\": \"vlan-test\"," +
                "\"type\": \"VLAN\"," +
                "\"segmentId\": \"100\"," +
                "\"physnetName\": \"external\"," +
                "\"mtu\": 1500," +
                "\"cidr\": \"20.20.20.0/24\"," +
                "\"gatewayIp\": \"20.20.20.1\"," +
                "\"ipPool\": {\"start\": \"20.20.20.2\", \"end\": \"20.20.20.254\"}" +
                "}";
        String raw = "{\"apiVersion\": \"k8s.cni.cncf.io/v1\"," +
                "\"kind\": \"NetworkAttachmentDefinition\"," +
                nadResource(config).substring(1);

        KubevirtNetwork expected = parseKubevirtNetwork(raw);
        KubevirtNetwork network = parseKubevirtNetwork(customResourceJson(genericResource(raw)));

        assertNotNull(network);
        assertEquals(expected.networkId(), network.networkId());
        assertEquals(expected.name(), network.name());
        assertEquals(expected.type(), network.type());
        assertEquals(expected.mtu(), network.mtu());
        assertEquals(expected.segmentId(), network.segmentId());
        assertEquals(expected.physnetName(), network.physnetName());
        assertEquals(expected.gatewayIp(), network.gatewayIp());
        assertEquals(expected.cidr(), network.cidr());
        assertEquals(expected.ipPool(), network.ipPool());
        assertEquals("vlan-test", parseResourceName(customResourceJson(genericResource(raw))));
    }

    /**
     * Tests that the top-level fields a generic resource only carries as
     * additional properties (spec, status) survive the round trip with their
     * JSON types intact, since the parsers read numbers with asInt().
     */
    @Test
    public void testCustomResourceJsonKeepsSpecAndStatus() throws IOException {
        String raw = "{\"apiVersion\": \"kubevirt.io/v1\"," +
                "\"kind\": \"VirtualRouter\"," +
                "\"metadata\": {\"name\": \"router-1\", \"namespace\": \"default\"}," +
                "\"spec\": {\"mtu\": 1500, \"internal\": [\"net-1\"], " +
                "\"gateway\": {\"name\": \"gw\", \"enabled\": true}}," +
                "\"status\": {\"phase\": \"Active\"}}";

        JsonNode json = new ObjectMapper().readTree(customResourceJson(genericResource(raw)));

        assertEquals("kubevirt.io/v1", json.get("apiVersion").asText());
        assertEquals("VirtualRouter", json.get("kind").asText());
        assertEquals("router-1", json.get("metadata").get("name").asText());
        assertTrue(json.get("spec").get("mtu").isInt());
        assertEquals(1500, json.get("spec").get("mtu").asInt());
        assertEquals("net-1", json.get("spec").get("internal").get(0).asText());
        assertTrue(json.get("spec").get("gateway").get("enabled").asBoolean());
        assertEquals("Active", json.get("status").get("phase").asText());
    }
}
