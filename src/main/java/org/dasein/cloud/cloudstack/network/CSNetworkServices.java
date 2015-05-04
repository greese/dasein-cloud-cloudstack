/**
 * Copyright (C) 2009-2015 Dell, Inc.
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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.network.AbstractNetworkServices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CSNetworkServices extends AbstractNetworkServices<CSCloud> {
    public CSNetworkServices(@Nonnull CSCloud provider) {
        super(provider);
    }

    @Override 
    public @Nullable SecurityGroup getFirewallSupport() {
        return new SecurityGroup(getProvider());
    }
    
    @Override
    public @Nullable IpAddress getIpAddressSupport() {
        try {
            if( getProvider().hasApi(IpAddress.ASSOCIATE_IP_ADDRESS) ) {
                return new IpAddress(getProvider());
            }
        }
        catch( CloudException e ) {
        }
        catch( InternalException e ) {
        }
        return null;
    }
    
    @Override
    public @Nullable LoadBalancers getLoadBalancerSupport() {
        try {
            if( getProvider().hasApi("createLoadBalancer") ) {
                return new LoadBalancers(getProvider());
            }
        }
        catch( CloudException e ) {
        }
        catch( InternalException e ) {
        }
        return null;
    }
    
    @Override
    public @Nullable Network getVlanSupport() {
        return new Network(getProvider());
    }
}
