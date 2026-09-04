/*
 * Copyright 2019-present Open Networking Foundation
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
package org.onosproject.k8snetworking.util;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import org.junit.Test;
import org.onlab.packet.IpAddress;

import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.onosproject.k8snetworking.util.K8sNetworkingUtil.existingContainerPortByMac;
import static org.onosproject.k8snetworking.util.K8sNetworkingUtil.getGatewayIp;
import static org.onosproject.k8snetworking.util.K8sNetworkingUtil.getSubnetIps;
import static org.onosproject.k8snetworking.util.K8sNetworkingUtil.kubernetesSerialization;

/**
 * Unit tests for kubernetes networking utils.
 */
public final class K8sNetworkUtilTest {

    /**
     * Tests the getSubnetIps method.
     */
    @Test
    public void testGetSubnetIps() {
        String bClassCidr = "10.10.0.0/16";
        Set<IpAddress> bClassIps = getSubnetIps(bClassCidr);
        assertEquals(((Double) Math.pow(2, 16)).intValue() - 4, bClassIps.size());

        String cClassCidr = "10.10.10.0/24";
        Set<IpAddress> cClassIps = getSubnetIps(cClassCidr);
        assertEquals(((Double) Math.pow(2, 8)).intValue() - 4, cClassIps.size());

        String dClassCidr = "10.10.10.10/32";
        Set<IpAddress> dClassIps = getSubnetIps(dClassCidr);
        assertEquals(0, dClassIps.size());
    }

    @Test
    public void testGetGatewayIp() {
        String classCidr = "10.10.10.0/24";
        IpAddress gatewayIp = getGatewayIp(classCidr);
        assertEquals("10.10.10.1", gatewayIp.toString());
    }

    /**
     * Tests the existing container port by MAC.
     */
    @Test
    public void testExistingContainerPortByMac() {
        String sourceMacStr = "fe:85:5a:d8:68:1d";
        String comparedMacStr = "8A:85:5A:D8:68:1D";

        boolean result1 = existingContainerPortByMac(sourceMacStr, comparedMacStr);
        boolean result2 = existingContainerPortByMac(comparedMacStr, sourceMacStr);

        assertTrue(result1);
        assertTrue(result2);

        String wrongMacStr = "8A:85:5A:D8:68:1F";
        boolean result3 = existingContainerPortByMac(sourceMacStr, wrongMacStr);
        boolean result4 = existingContainerPortByMac(wrongMacStr, sourceMacStr);

        assertFalse(result3);
        assertFalse(result4);
    }

    private static String watchEvent(String apiVersion, String kind) {
        return "{\"type\": \"ADDED\", \"object\": {" +
                "\"apiVersion\": \"" + apiVersion + "\", \"kind\": \"" + kind + "\"," +
                "\"metadata\": {\"name\": \"test\", \"namespace\": \"default\"}}}";
    }

    /**
     * Tests that the serialization handed to the client maps every kind this
     * application watches to its model class, so watch events arrive typed
     * even where the deserializer's ServiceLoader lookup finds nothing.
     */
    @Test
    public void testKubernetesSerializationRegistersWatchedKinds() {
        KubernetesSerialization serialization = kubernetesSerialization();

        assertEquals(NetworkPolicy.class,
                serialization.getRegisteredKubernetesResource("networking.k8s.io/v1", "NetworkPolicy"));
        assertEquals(Ingress.class,
                serialization.getRegisteredKubernetesResource("networking.k8s.io/v1", "Ingress"));
        assertEquals(Pod.class, serialization.getRegisteredKubernetesResource("v1", "Pod"));

        WatchEvent policyEvent = serialization.unmarshal(
                watchEvent("networking.k8s.io/v1", "NetworkPolicy"), WatchEvent.class);
        assertTrue(policyEvent.getObject() instanceof NetworkPolicy);

        WatchEvent ingressEvent = serialization.unmarshal(
                watchEvent("networking.k8s.io/v1", "Ingress"), WatchEvent.class);
        assertTrue(ingressEvent.getObject() instanceof Ingress);

        // an unregistered kind is not an error; it just comes back untyped
        WatchEvent unknownEvent = serialization.unmarshal(
                watchEvent("example.io/v1", "Unknown"), WatchEvent.class);
        assertTrue(unknownEvent.getObject() instanceof GenericKubernetesResource);
    }
}
