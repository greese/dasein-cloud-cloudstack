/**
 * Copyright (C) 2009-2013 enstratius, Inc.
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
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.network.IPAddressCapabilities;
import org.dasein.cloud.network.IPVersion;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudstack with respect to Dasein ip address operations.
 * <p>Created by Danielle Mayne: 3/04/14 10:11 AM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class CSIPAddressCapabilities extends AbstractCapabilities<CSCloud> implements IPAddressCapabilities {
    public CSIPAddressCapabilities(CSCloud cloud) {super(cloud);}

    @Nonnull
    @Override
    public String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "IP address";
    }

    @Nonnull
    @Override
    public Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean canBeAssigned(@Nonnull VmState vmState) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return IPVersion.IPV4.equals(ofVersion);
    }
}
