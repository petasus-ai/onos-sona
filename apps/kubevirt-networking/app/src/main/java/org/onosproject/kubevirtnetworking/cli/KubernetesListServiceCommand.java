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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLb;
import org.onosproject.kubevirtnetworking.api.KubernetesExternalLbService;

import java.util.List;
import java.util.stream.Collectors;

import static org.onosproject.kubevirtnetworking.api.Constants.CLI_IP_ADDRESS_LENGTH;
import static org.onosproject.kubevirtnetworking.api.Constants.CLI_LONG_SERVICE_PORT_LENGTH;
import static org.onosproject.kubevirtnetworking.api.Constants.CLI_MAC_ADDRESS_LENGTH;
import static org.onosproject.kubevirtnetworking.api.Constants.CLI_MARGIN_LENGTH;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.genColumnLength;
import static org.onosproject.kubevirtnetworking.util.KubevirtNetworkingUtil.genFormatString;

/**
 * Lists kubernetes services.
 */
@Service
@Command(scope = "onos", name = "kubernetes-services",
        description = "Lists all kubernetes services")
public class KubernetesListServiceCommand extends AbstractShellCommand {

    @Override
    protected void doExecute() throws Exception {
        KubernetesExternalLbService service = get(KubernetesExternalLbService.class);

        List<KubernetesExternalLb> elbList = Lists.newArrayList(service.loadBalancers());

        int serviceNameLength = genColumnLength("Service Name", elbList.stream()
                .map(KubernetesExternalLb::serviceName).collect(Collectors.toList()));
        int gatewayLength = genColumnLength("Elected Gateway", elbList.stream()
                .map(lb -> lb.electedGateway() == null ? "N/A" : lb.electedGateway())
                .collect(Collectors.toList()));
        int workerLength = genColumnLength("Elected Worker", elbList.stream()
                .map(lb -> lb.electedWorker() == null ? "N/A" : lb.electedWorker())
                .collect(Collectors.toList()));

        String format = genFormatString(ImmutableList.of(serviceNameLength, CLI_IP_ADDRESS_LENGTH,
                gatewayLength, workerLength, CLI_IP_ADDRESS_LENGTH, CLI_MAC_ADDRESS_LENGTH,
                CLI_LONG_SERVICE_PORT_LENGTH));

        print(format, "Service Name", "Loadbalancer IP", "Elected Gateway", "Elected Worker",
                "Loadbalancer GW IP", "Loadbalancer GW MAC", "Service Port");

        for (KubernetesExternalLb elb : elbList) {
            String lbIp = elb.loadBalancerIp() == null ? "N/A" : elb.loadBalancerIp().toString();
            String electedGw = elb.electedGateway() == null ? "N/A" : elb.electedGateway();
            String electedWorker = elb.electedWorker() == null ? "N/A" : elb.electedWorker();
            String lbGwIp = elb.loadBalancerGwIp() == null ? "N/A" : elb.loadBalancerGwIp().toString();
            String lbGwMac = elb.loadBalancerGwMac() == null ? "N/A" : elb.loadBalancerGwMac().toString();
            String lbServicePort = elb.servicePorts().isEmpty() ? "N/A" : elb.servicePorts().toString();

            print(format, elb.serviceName(),
                    StringUtils.substring(lbIp, 0,
                            CLI_IP_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                    electedGw,
                    electedWorker,
                    StringUtils.substring(lbGwIp, 0,
                            CLI_IP_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                    StringUtils.substring(lbGwMac, 0,
                            CLI_MAC_ADDRESS_LENGTH - CLI_MARGIN_LENGTH),
                    StringUtils.substring(lbServicePort, 0,
                            CLI_LONG_SERVICE_PORT_LENGTH - CLI_MARGIN_LENGTH)
            );
        }
    }
}
