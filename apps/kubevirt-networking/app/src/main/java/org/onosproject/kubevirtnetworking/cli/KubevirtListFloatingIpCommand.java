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
package org.onosproject.kubevirtnetworking.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.kubevirtnetworking.api.KubevirtFloatingIp;
import org.onosproject.kubevirtnetworking.api.KubevirtRouterService;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.onosproject.kubevirtnetworking.api.Constants.CLI_IP_ADDRESS_LENGTH;
import static org.onosproject.kubevirtnetworking.api.Constants.CLI_MARGIN_LENGTH;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.genColumnLength;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.genFormatString;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.prettyJson;

/**
 * Lists kubevirt floating IPs.
 */
@Service
@Command(scope = "onos", name = "kubevirt-fips",
        description = "Lists all kubevirt floating IPs")
public class KubevirtListFloatingIpCommand extends AbstractShellCommand {

    @Override
    protected void doExecute() throws Exception {
        KubevirtRouterService service = get(KubevirtRouterService.class);
        List<KubevirtFloatingIp> fips = Lists.newArrayList(service.floatingIps());
        fips.sort(Comparator.comparing(KubevirtFloatingIp::networkName));

        int networkNameLength = genColumnLength("Network Name", fips.stream()
                .map(KubevirtFloatingIp::networkName).collect(Collectors.toList()));
        int podNameLength = genColumnLength("POD Name", fips.stream()
                .map(f -> f.podName() == null ? "N/A" : f.podName())
                .collect(Collectors.toList()));
        int vmNameLength = genColumnLength("VM Name", fips.stream()
                .map(f -> f.vmName() == null ? "N/A" : f.vmName())
                .collect(Collectors.toList()));

        String format = genFormatString(ImmutableList.of(networkNameLength,
                CLI_IP_ADDRESS_LENGTH, podNameLength, vmNameLength, CLI_IP_ADDRESS_LENGTH));

        if (outputJson()) {
            print("%s", json(fips));
        } else {
            print(format, "Network Name", "Floating IP", "POD Name", "VM Name", "Fixed IP");
            for (KubevirtFloatingIp fip : fips) {

                String fixedIp = fip.fixedIp() == null ? "N/A" : fip.fixedIp().toString();
                String podName = fip.podName() == null ? "N/A" : fip.podName();
                String vmName = fip.vmName() == null ? "N/A" : fip.vmName();

                print(format, fip.networkName(),
                        StringUtils.substring(fip.floatingIp().toString(), 0,
                                CLI_IP_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                        podName,
                        vmName,
                        StringUtils.substring(fixedIp, 0,
                                CLI_IP_ADDRESS_LENGTH - CLI_MARGIN_LENGTH)
                );
            }
        }
    }

    private String json(List<KubevirtFloatingIp> fips) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode result = mapper.createArrayNode();

        for (KubevirtFloatingIp fip : fips) {
            result.add(jsonForEntity(fip, KubevirtFloatingIp.class));
        }

        return prettyJson(mapper, result.toString());
    }
}
