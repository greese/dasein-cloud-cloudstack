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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.compute.AbstractComputeServices;

import javax.annotation.Nonnull;

public class CSComputeServices extends AbstractComputeServices<CSCloud> {
    public CSComputeServices(@Nonnull CSCloud cloud) { super(cloud); }
    
    @Override
    public @Nonnull Templates getImageSupport() {
        return new Templates(getProvider());
    }
    
    @Override
    public @Nonnull Snapshots getSnapshotSupport() {
        try {
            if( getProvider().hasApi("createSnapshot") ) {
                return new Snapshots(getProvider());
            }
        }
        catch( CloudException ignore ) {
        }
        catch( InternalException ignore ) {
        }
        return null;
    }
    
    @Override
    public @Nonnull VirtualMachines getVirtualMachineSupport() {
        return new VirtualMachines(getProvider());
    }
    
    @Override
    public @Nonnull Volumes getVolumeSupport() {
        try {
            if( getProvider().hasApi("createVolume") ) {
                return new Volumes(getProvider());
            }
        }
        catch( CloudException ignore ) {
        }
        catch( InternalException ignore ) {
        }
        return null;
    }
}
