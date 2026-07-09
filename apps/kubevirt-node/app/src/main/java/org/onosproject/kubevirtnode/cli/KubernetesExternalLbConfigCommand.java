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
package org.onosproject.kubevirtnode.cli;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.onlab.packet.IpAddress;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfig;
import org.onosproject.kubevirtnode.api.KubernetesExternalLbConfigService;

import java.util.Collections;

import static org.onosproject.kubevirtnode.api.Constants.CLI_IP_ADDRESSES_LENGTH;
import static org.onosproject.kubevirtnode.api.Constants.CLI_IP_ADDRESS_LENGTH;
import static org.onosproject.kubevirtnode.api.Constants.CLI_MAC_ADDRESS_LENGTH;
import static org.onosproject.kubevirtnode.api.Constants.CLI_MARGIN_LENGTH;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.genColumnLength;
import static org.onosproject.kubevirtnode.util.KubevirtNodeUtil.genFormatString;

/**
 * Lists all Kubernetes External LB config registered to the service.
 */
@Service
@Command(scope = "onos", name = "kubernetes-lb-configs",
        description = "Lists all Kubernetes External LB config registered to the service")
public class KubernetesExternalLbConfigCommand extends AbstractShellCommand {

    private static final String KUBE_VIP = "kubevip";

    @Override
    protected void doExecute() throws Exception {
        KubernetesExternalLbConfigService service = get(KubernetesExternalLbConfigService.class);

        KubernetesExternalLbConfig lbConfig = service.lbConfig(KUBE_VIP);

        if (lbConfig == null) {
            print("LB config not found!");
        } else {
            String configName = lbConfig.configName();

            int configNameLength = genColumnLength("ConfigName",
                    Collections.singletonList(configName));

            String format = genFormatString(ImmutableList.of(configNameLength,
                    CLI_IP_ADDRESS_LENGTH, CLI_MAC_ADDRESS_LENGTH, CLI_IP_ADDRESSES_LENGTH));

            print(format, "ConfigName", "Gateway IP", "Gateway MAC", "Global-Range");

            IpAddress gatewayIp = lbConfig.loadBalancerGwIp();
            String gatewayMac = lbConfig.loadBalancerGwMac() == null ? "N/A" : lbConfig.loadBalancerGwMac().toString();
            String globalRange = lbConfig.globalIpRange() == null ? "N/A" : lbConfig.globalIpRange();

            print(format, configName,
                    StringUtils.substring(gatewayIp.toString(), 0,
                           CLI_IP_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                    StringUtils.substring(gatewayMac, 0,
                            CLI_MAC_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                    StringUtils.substring(globalRange, 0,
                            CLI_IP_ADDRESSES_LENGTH - CLI_MARGIN_LENGTH)
                    );
        }
    }
}
