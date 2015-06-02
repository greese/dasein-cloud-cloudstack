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

package org.dasein.cloud.cloudstack.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.VisibleScope;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.compute.SnapshotCapabilities;
import org.dasein.cloud.util.NamingConstraints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudstack with respect to Dasein snapshot operations.
 * User: daniellemayne
 * Date: 05/03/2014
 * Time: 15:39
 */
public class CSSnapshotCapabilities extends AbstractCapabilities<CSCloud> implements SnapshotCapabilities{
    public CSSnapshotCapabilities(@Nonnull CSCloud provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Nullable
    @Override
    public VisibleScope getSnapshotVisibleScope() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean supportsSnapshotCopying() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return false;
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull NamingConstraints getSnapshotNamingConstraints() throws CloudException, InternalException {
        // not sure what these are from the api docs, but from the UI they don't seem
        // to restrict on much of anything
        return NamingConstraints.getAlphaNumeric(1, 255);
    }
}
