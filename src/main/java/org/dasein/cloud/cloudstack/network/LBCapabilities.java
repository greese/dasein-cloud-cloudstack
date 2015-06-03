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

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.VisibleScope;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.LbAlgorithm;
import org.dasein.cloud.network.LbEndpointType;
import org.dasein.cloud.network.LbPersistence;
import org.dasein.cloud.network.LbProtocol;
import org.dasein.cloud.network.LoadBalancerAddressType;
import org.dasein.cloud.network.LoadBalancerCapabilities;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudstack with respect to Dasein load balancer operations.
 * <p>Created by Danielle Mayne: 3/04/14 10:11 AM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class LBCapabilities extends AbstractCapabilities<CSCloud> implements LoadBalancerCapabilities {
    public LBCapabilities(@Nonnull CSCloud provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 0;
    }

    @Nonnull
    @Override
    public String getProviderTermForLoadBalancer(@Nonnull Locale locale) {
        return "load balancer";
    }

    @Nullable
    @Override
    public VisibleScope getLoadBalancerVisibleScope() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean healthCheckRequiresLoadBalancer() throws CloudException, InternalException {
        return true;
    }

    @Override
    public Requirement healthCheckRequiresName() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Nonnull
    @Override
    public Requirement identifyEndpointsOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Nonnull
    @Override
    public Requirement identifyListenersOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public @Nonnull Requirement identifyVlanOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public @Nonnull Requirement identifyHealthCheckOnCreateRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    static private volatile List<LbAlgorithm> algorithms = null;
    @Nonnull
    @Override
    public Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        List<LbAlgorithm> list = algorithms;

        if( list == null ) {
            list = new ArrayList<LbAlgorithm>();
            list.add(LbAlgorithm.ROUND_ROBIN);
            list.add(LbAlgorithm.LEAST_CONN);
            list.add(LbAlgorithm.SOURCE);
            algorithms = Collections.unmodifiableList(list);
        }
        return algorithms;
    }

    @Nonnull
    @Override
    public Iterable<LbEndpointType> listSupportedEndpointTypes() throws CloudException, InternalException {
        return Collections.singletonList(LbEndpointType.VM);
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Nonnull
    @Override
    public Iterable<LbPersistence> listSupportedPersistenceOptions() throws CloudException, InternalException {
        return Collections.singletonList(LbPersistence.NONE);
    }

    static private volatile List<LbProtocol> protocols = null;
    @Nonnull
    @Override
    public Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        if( protocols == null ) {
            List<LbProtocol> list = new ArrayList<LbProtocol>();

            list.add(LbProtocol.RAW_TCP);
            protocols = Collections.unmodifiableList(list);
        }
        return protocols;
    }

    @Override
    public boolean supportsAddingEndpoints() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsMonitoring() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsMultipleTrafficTypes() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull NamingConstraints getLoadBalancerNamingConstraints() throws CloudException, InternalException {
        // not sure what these are from the api docs, but from the UI they don't seem
        // to restrict on much of anything
        return NamingConstraints.getAlphaNumeric(1, 255);
    }
}
