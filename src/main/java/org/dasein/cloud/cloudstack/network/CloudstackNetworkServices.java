/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.cloudstack.network;

import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.cloudstack.CloudstackVersion;
import org.dasein.cloud.cloudstack.ServiceProvider;
import org.dasein.cloud.network.AbstractNetworkServices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CloudstackNetworkServices extends AbstractNetworkServices {
    private CloudstackProvider cloud;
    
    public CloudstackNetworkServices(@Nonnull CloudstackProvider cloud) { this.cloud = cloud; }
    
    @Override 
    public @Nullable SecurityGroup getFirewallSupport() {
        return new SecurityGroup(cloud);
    }
    
    @Override
    public @Nullable IpAddress getIpAddressSupport() {
        if( cloud.getServiceProvider().equals(ServiceProvider.DATAPIPE) ) {
            return null;
        }
        return new IpAddress(cloud);
    }
    
    @Override
    public @Nullable LoadBalancers getLoadBalancerSupport() {
        if( cloud.getServiceProvider().equals(ServiceProvider.DATAPIPE) ) {
            return null;
        }
        return new LoadBalancers(cloud);
    }
    
    @Override
    public @Nullable Network getVlanSupport() {
        if( cloud.getServiceProvider().equals(ServiceProvider.DATAPIPE) ) {
            return null;
        }
        return new Network(cloud);
    }
}
