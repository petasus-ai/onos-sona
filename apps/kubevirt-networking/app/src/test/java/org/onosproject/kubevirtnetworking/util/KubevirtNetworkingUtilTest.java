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

import org.junit.Test;
import org.onlab.packet.IpAddress;
import org.onosproject.kubevirtnetworking.api.KubevirtNetwork;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.parseKubevirtNetwork;
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
}
