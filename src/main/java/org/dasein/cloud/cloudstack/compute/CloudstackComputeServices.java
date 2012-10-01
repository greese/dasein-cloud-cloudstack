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

package org.dasein.cloud.cloudstack.compute;

import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.compute.AbstractComputeServices;

import javax.annotation.Nonnull;

public class CloudstackComputeServices extends AbstractComputeServices {
    private CloudstackProvider cloud = null;
    
    public CloudstackComputeServices(@Nonnull CloudstackProvider cloud) { this.cloud = cloud; }
    
    @Override
    public @Nonnull Templates getImageSupport() {
        return new Templates(cloud);
    }
    
    @Override
    public @Nonnull Snapshots getSnapshotSupport() {
        return new Snapshots(cloud);
    }
    
    @Override
    public @Nonnull VirtualMachines getVirtualMachineSupport() {
        return new VirtualMachines(cloud);
    }
    
    @Override
    public @Nonnull Volumes getVolumeSupport() {
        return new Volumes(cloud);
    }
}
