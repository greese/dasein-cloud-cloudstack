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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.CSVersion;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.compute.Snapshot;
import org.dasein.cloud.compute.SnapshotState;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.CalendarWrapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Snapshots implements SnapshotSupport {
    static private final Logger logger = Logger.getLogger(Snapshots.class);
    
    static private final String CREATE_SNAPSHOT = "createSnapshot";
    static private final String DELETE_SNAPSHOT = "deleteSnapshot";
    static private final String LIST_SNAPSHOTS  = "listSnapshots";
    
    private CSCloud provider;
    
    Snapshots(CSCloud provider) {
        this.provider = provider;
    }

    @Override
    public void addSnapshotShare(@Nonnull String providerSnapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Snapshot sharing is not supported");
    }

    @Override
    public void addPublicShare(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Snapshot sharing is not supported");
    }

    @Override
    @Deprecated
    public @Nonnull String create(@Nonnull String volumeId, @Nonnull String description) throws InternalException, CloudException {
        @SuppressWarnings("ConstantConditions") String name = (description == null ? ("From " + volumeId) : description);

        return snapshot(volumeId, name, name).getProviderSnapshotId();
    }

    @Override
    public @Nullable Snapshot getSnapshot(@Nonnull String snapshotId) throws InternalException, CloudException {
        for( Snapshot snapshot : listSnapshots() ) {
            if( snapshot.getProviderSnapshotId().equals(snapshotId) ) {
                return snapshot;
            }
        }
        return null;
    }

    @Override
    public void remove(@Nonnull String snapshotId) throws InternalException, CloudException {
        CSMethod method = new CSMethod(provider);
        String url = method.buildUrl(DELETE_SNAPSHOT, new Param("id", snapshotId));
        Document doc;
        
        doc = method.get(url);
        provider.waitForJob(doc, "Delete Snapshot");
    }

    @Override
    public void removeAllSnapshotShares(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Snapshot sharing is not supported");
    }

    @Override
    public void removeSnapshotShare(@Nonnull String providerSnapshotId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Snapshot sharing is not supported");
    }

    @Override
    public void removePublicShare(@Nonnull String providerSnapshotId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Snapshot sharing is not supported");
    }

    @Override
    public @Nonnull Iterable<Snapshot> searchSnapshots(@Nullable String ownerId, @Nullable String keyword) throws InternalException, CloudException {
        ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();

        if( keyword != null ) {
            keyword = keyword.toLowerCase();
        }
        for( Snapshot s : listSnapshots() ) {
            if( ownerId != null && !ownerId.equals(s.getOwner()) ) {
                continue;
            }
            if( keyword != null && (!s.getName().toLowerCase().contains(keyword) && !s.getDescription().toLowerCase().contains(keyword)) ) {
                continue;
            }
            snapshots.add(s);
        }
        return snapshots;
    }

    @Override
    public @Nonnull String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String snapshotId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listSnapshotStatus() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(provider);
        String url = method.buildUrl(LIST_SNAPSHOTS, new Param("zoneId", ctx.getRegionId()));
        Document doc;

        doc = method.get(url);
        ArrayList<ResourceStatus> snapshots = new ArrayList<ResourceStatus>();
        NodeList matches = doc.getElementsByTagName("snapshot");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node s = matches.item(i);

            if( s != null ) {
                ResourceStatus snapshot = toStatus(s);

                if( snapshot != null ) {
                    snapshots.add(snapshot);
                }
            }
        }
        return snapshots;
    }

    @Override
    public @Nonnull Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isPublic(@Nonnull String snapshotId) throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
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
    public @Nonnull Iterable<Snapshot> listSnapshots() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        Iterable<Volume> volumes = provider.getComputeServices().getVolumeSupport().listVolumes();
        CSMethod method = new CSMethod(provider);
        String url = method.buildUrl(LIST_SNAPSHOTS, new Param("zoneId", ctx.getRegionId()));
        Document doc;
        
        doc = method.get(url);
        ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();
        NodeList matches = doc.getElementsByTagName("snapshot");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node s = matches.item(i);

            if( s != null ) {
                Snapshot snapshot = toSnapshot(s, ctx, volumes);
                
                if( snapshot != null ) {
                    snapshots.add(snapshot);
                }
            }
        }
        return snapshots;
    }

    private Snapshot getLatestSnapshot(String forVolumeId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(provider);
        String url = method.buildUrl(LIST_SNAPSHOTS, new Param("zoneId", ctx.getRegionId()), new Param("volumeId", forVolumeId));
        Volume volume = provider.getComputeServices().getVolumeSupport().getVolume(forVolumeId);
        List<Volume> volumes;
        Document doc;

        if( volume == null ) {
            volumes = Collections.emptyList();
        }
        else {
            volumes = Collections.singletonList(volume);
        }
        doc = method.get(url);
        Snapshot latest = null;
        
        NodeList matches = doc.getElementsByTagName("snapshot");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node s = matches.item(i);

            if( s != null ) {
                Snapshot snapshot = toSnapshot(s, ctx, volumes);
                
                if( snapshot != null && snapshot.getVolumeId() != null && snapshot.getVolumeId().equals(forVolumeId) ) {
                    if( latest == null || snapshot.getSnapshotTimestamp() > latest.getSnapshotTimestamp() ) {
                        latest = snapshot;
                    }
                }
            }
        }
        return latest;
    }
    
    @Override
    public void shareSnapshot(@Nonnull String arg0, String arg1, boolean arg2) throws InternalException, CloudException {
        throw new OperationNotSupportedException();
    }

    @Override
    public @Nonnull Snapshot snapshot(@Nonnull String volumeId, @Nonnull String name, @Nonnull String description, @Nullable Tag... tags) throws InternalException, CloudException {
        Volume volume = provider.getComputeServices().getVolumeSupport().getVolume(volumeId);

        if( volume == null ) {
            throw new CloudException("No such volume: " + volumeId);
        }
        if( volume.getProviderVirtualMachineId() == null ) {
            throw new CloudException("You must attach this volume before you can snapshot it.");
        }
        long timeout = System.currentTimeMillis() +(CalendarWrapper.MINUTE * 10L);

        while( timeout > System.currentTimeMillis() ) {
            if( VolumeState.AVAILABLE.equals(volume.getCurrentState()) ) {
                break;
            }
            if( VolumeState.DELETED.equals(volume.getCurrentState()) ) {
                throw new CloudException("Volume " + volumeId + " disappeared before a snapshot could be taken");
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
            try { volume = provider.getComputeServices().getVolumeSupport().getVolume(volumeId); }
            catch( Throwable ignore ) { }
            if( volume == null ) {
                throw new CloudException("Volume " + volumeId + " disappeared before a snapshot could be taken");
            }
        }

        CSMethod method = new CSMethod(provider);
        String url = method.buildUrl(CREATE_SNAPSHOT, new Param("volumeId", volumeId));
        Document doc;

        try {
            doc = method.get(url);
        }
        catch( CSException e ) {
            int code = e.getHttpCode();

            if( code == 431 && e.getMessage() != null && e.getMessage().contains("no change since last snapshot")) {
                Snapshot s = getLatestSnapshot(volumeId);

                if( s == null ) {
                    throw e;
                }
                return s;
            }
            else if( provider.getVersion().equals(CSVersion.CS21) && (code == 500 || code == 530) ) {
                if( e.getMessage() != null && e.getMessage().contains("Snapshot could not be scheduled") ) {
                    // a couple of problems here...
                    // this is not really an error condition, so we should look for the current in-progress snapshot
                    // but cloud.com does not list in-progress snapshots
                    long then = (System.currentTimeMillis() - (CalendarWrapper.MINUTE*9));
                    long now = System.currentTimeMillis() - CalendarWrapper.MINUTE;
                    Snapshot wtf = null;

                    timeout = (now + (CalendarWrapper.MINUTE*20));
                    while( System.currentTimeMillis() < timeout ) {
                        Snapshot latest = getLatestSnapshot(volumeId);

                        if( latest != null && latest.getSnapshotTimestamp() >= now ) {
                            return latest;
                        }
                        else if( latest != null && latest.getSnapshotTimestamp() >= then ) {
                            wtf = latest;
                        }
                        try { Thread.sleep(20000L); }
                        catch( InterruptedException ignore ) { /* ignore */ }
                    }
                    if( wtf != null ) {
                        return wtf;
                    }
                    return snapshot(volumeId, name, description, tags);
                }
            }
            throw e;
        }
        NodeList matches;

        if( provider.getVersion().greaterThan(CSVersion.CS21) ) {
            matches = doc.getElementsByTagName("id");
        }
        else {
            matches = doc.getElementsByTagName("snapshotid");
            if( matches.getLength() < 1 ) {
                matches = doc.getElementsByTagName("id");
            }
        }
        String snapshotId = null;

        if( matches.getLength() > 0 ) {
            snapshotId = matches.item(0).getFirstChild().getNodeValue();
        }
        if( snapshotId == null ) {
            throw new CloudException("Failed to create a snapshot");
        }
        try {
            provider.waitForJob(doc, "Create Snapshot");
        }
        catch( CSException e ) {
            if( e.getHttpCode() == 431 ) {
                logger.warn("CSCloud opted not to make a snapshot: " + e.getMessage());
                Snapshot s = getLatestSnapshot(volumeId);

                if( s == null ) {
                    throw e;
                }
                return s;
            }
            throw e;
        }
        catch( CloudException e ) {
            String msg = e.getMessage();

            if( msg != null && msg.contains("no change since last snapshot") ) {
                Snapshot s = getLatestSnapshot(volumeId);

                if( s == null ) {
                    throw e;
                }
                return s;
            }
            throw e;
        }
        Snapshot s = getSnapshot(snapshotId);

        if( s == null ) {
            throw new CloudException("Created snapshot " + snapshotId + " but it does not really exist");
        }
        return s;
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    private @Nullable Snapshot toSnapshot(@Nullable Node node, @Nonnull ProviderContext ctx, @Nonnull Iterable<Volume> volumes) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        Snapshot snapshot = new Snapshot();
        NodeList attrs = node.getChildNodes();
        String type = null;

        snapshot.setCurrentState(SnapshotState.AVAILABLE);
        snapshot.setOwner(ctx.getAccountNumber());
        snapshot.setProgress("100%");
        snapshot.setRegionId(ctx.getRegionId());
        snapshot.setSizeInGb(1);
        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);

            String name = attr.getNodeName();
            String value = null;

            if( attr.hasChildNodes() ) {
                value = attr.getFirstChild().getNodeValue();
            }
            if( name.equalsIgnoreCase("id") ) {
                snapshot.setProviderSnapshotId(value);
            }
            else if( name.equalsIgnoreCase("volumeid") ) {
                snapshot.setVolumeId(value);
                if( value != null ) {
                    Volume v = null;

                    for( Volume volume : volumes ) {
                        if( volume.getProviderVolumeId().equals(value) ) {
                            v = volume;
                            break;
                        }
                    }
                    if( v != null ) {
                        snapshot.setSizeInGb(v.getSize().intValue());
                    }
                }
            }
            else if( name.equalsIgnoreCase("name") ) {
                snapshot.setName(value);
            }
            else if( value != null && name.equalsIgnoreCase("created") ) {
                snapshot.setSnapshotTimestamp(provider.parseTime(value));
            }
            else if( name.equalsIgnoreCase("snapshottype") ) {
                type = value;
            }
            else if( name.equals("account") ) {
                snapshot.setOwner(value);
            }
        }
        if( snapshot.getProviderSnapshotId() == null ) {
            return null;
        }
        if( snapshot.getName() == null ) {
            snapshot.setName(snapshot.getProviderSnapshotId());
        }
        if( snapshot.getDescription() == null ) {
            if( type == null ) {
                snapshot.setDescription(snapshot.getName());
            }
            else {
                snapshot.setDescription(snapshot.getName() + " (" + type + ")");
            }
        }
        return snapshot;
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attrs = node.getChildNodes();
        String snapId = null;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);

            String name = attr.getNodeName();
            String value = null;

            if( attr.hasChildNodes() ) {
                value = attr.getFirstChild().getNodeValue();
            }
            if( name.equalsIgnoreCase("id") ) {
                snapId = value;
                break;
            }
        }
        if( snapId == null ) {
            return null;
        }
        return new ResourceStatus(snapId, SnapshotState.AVAILABLE);
    }


    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        return provider.getComputeServices().getVolumeSupport().isSubscribed();
    }
}