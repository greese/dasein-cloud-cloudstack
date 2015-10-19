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

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.cloudstack.*;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Volumes extends AbstractVolumeSupport<CSCloud> {
    static private final Logger logger = Logger.getLogger(Volumes.class);
    
    static private final String ATTACH_VOLUME = "attachVolume";
    static private final String CREATE_VOLUME = "createVolume";
    static private final String DELETE_VOLUME = "deleteVolume";
    static private final String DETACH_VOLUME = "detachVolume";
    static private final String LIST_DISK_OFFERINGS     = "listDiskOfferings";

    static private final String LIST_VOLUMES  = "listVolumes";

    static public class DiskOffering {
        public String id;
        public long diskSize;
        public String name;
        public String description;
        public String type;

        public String toString() {return "DiskOffering ["+id+"] of size "+diskSize;}
    }
    
    Volumes(CSCloud provider) {
        super(provider);
    }
    
    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String serverId, @Nullable String deviceId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.attach");
        try {
            if( logger.isInfoEnabled() ) {
                logger.info("attaching " + volumeId + " to " + serverId + " as " + deviceId);
            }
            VirtualMachine vm = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + serverId);
            }
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 10L);

            while( timeout > System.currentTimeMillis() ) {
                if( VmState.RUNNING.equals(vm.getCurrentState()) || VmState.STOPPED.equals(vm.getCurrentState()) ) {
                    break;
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                try { vm = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId); }
                catch( Throwable ignore ) { }
                if( vm == null ) {
                    throw new CloudException("Virtual machine " + serverId + " disappeared waiting for it to enter an attachable state");
                }
            }
            List<Param> params = new ArrayList<Param>();
            params.add(new Param("id", volumeId));
            params.add(new Param("virtualMachineId", serverId));

            if( deviceId != null ) {
                deviceId = toDeviceNumber(deviceId);
                if( logger.isDebugEnabled() ) {
                    logger.debug("Device mapping is: " + deviceId);
                }
                params.add(new Param("deviceId", deviceId));
            }
            Document doc = new CSMethod(getProvider()).get(ATTACH_VOLUME, params);

            if( doc == null ) {
                throw new CloudException("No such volume or server");
            }
            getProvider().waitForJob(doc, "Attach Volume");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.createVolume");
        try {
            if( options.getFormat().equals(VolumeFormat.NFS) || !getProvider().hasApi("createVolume")) {
                throw new OperationNotSupportedException("NFS volumes are not currently supported in " + getProvider().getCloudName());
            }
            String snapshotId = options.getSnapshotId();
            String productId = options.getVolumeProductId();
            VolumeProduct product = null;

            if( productId != null ) {
                for( VolumeProduct prd : listVolumeProducts() ) {
                    if( productId.equals(prd.getProviderProductId()) ) {
                        product = prd;
                        break;
                    }
                }
            }
            Storage<Gigabyte> size;

            if( snapshotId == null ) {
                if( product == null ) {
                    size = options.getVolumeSize();
                    if( size.intValue() < getCapabilities().getMinimumVolumeSize().intValue() ) {
                        size = getCapabilities().getMinimumVolumeSize();
                    }
                    Iterable<VolumeProduct> products = listVolumeProducts();
                    VolumeProduct best = null;
                    VolumeProduct custom = null;

                    for( VolumeProduct p : products ) {
                        Storage<Gigabyte> s = p.getMinVolumeSize();

                        if( s  == null || s.intValue() == 0 ) {
                            if (custom == null) {
                                custom = p;
                            }
                            continue;
                        }
                        long currentSize = s.getQuantity().longValue();

                        s = (best == null ? null : best.getMinVolumeSize());

                        long bestSize = (s == null ? 0L : s.getQuantity().longValue());

                        if( size.longValue() > 0L && size.longValue() == currentSize ) {
                            product = p;
                            break;
                        }
                        if( best == null ) {
                            best = p;
                        }
                        else if( bestSize > 0L || currentSize > 0L ) {
                            if( size.longValue() > 0L ) {
                                if( bestSize < size.longValue() && bestSize >0L && (currentSize > size.longValue() || currentSize > bestSize) ) {
                                    best = p;
                                }
                                else if( bestSize > size.longValue() && currentSize > size.longValue() && currentSize < bestSize ) {
                                    best = p;
                                }
                            }
                            else if( currentSize > 0L && currentSize < bestSize ) {
                                best = p;
                            }
                        }
                    }
                    if( product == null ) {
                        if (custom != null) {
                            product = custom;
                        }
                        else {
                            product = best;
                        }
                    }
                }
                else {
                    size = product.getMinVolumeSize();
                    if( size == null || size.intValue() < 1 ) {
                        size = options.getVolumeSize();
                    }
                }
                if( product == null && size.longValue() < 1L ) {
                    throw new CloudException("No offering matching " + options.getVolumeProductId());
                }
            }
            else {
                Snapshot snapshot = getProvider().getComputeServices().getSnapshotSupport().getSnapshot(snapshotId);

                if( snapshot == null ) {
                    throw new CloudException("No such snapshot: " + snapshotId);
                }
                int s = snapshot.getSizeInGb();

                if( s < 1 || s < getCapabilities().getMinimumVolumeSize().intValue() ) {
                    size = getCapabilities().getMinimumVolumeSize();
                }
                else {
                    size = new Storage<>(s, Storage.GIGABYTE);
                }
            }
            List<Param> params = new ArrayList<>();
            params.add(new Param("name", options.getName()));
            params.add(new Param("zoneId", getContext().getRegionId()));

            if( product == null && snapshotId == null ) {
                /*params = new Param[] {
                        new Param("name", options.getName()),
                        new Param("zoneId", ctx.getRegionId()),
                        new Param("size", String.valueOf(size.longValue()))
                }; */
                throw new CloudException("A suitable snapshot or disk offering could not be found to pass to CloudStack createVolume request");
            }
            else if( snapshotId != null ) {
                params.add(new Param("snapshotId", snapshotId));
                params.add(new Param("size", String.valueOf(size.longValue())));
            }
            else {
                Storage<Gigabyte> s = product.getMinVolumeSize();
                params.add(new Param("diskOfferingId", product.getProviderProductId()));

                if( s == null || s.intValue() < 1 ) {
                    params.add(new Param("size", String.valueOf(size.longValue())));
                }
            }

            Document doc = new CSMethod(getProvider()).get(CREATE_VOLUME, params);
            NodeList matches = doc.getElementsByTagName("volumeid"); // v2.1
            String volumeId = null;

            if( matches.getLength() > 0 ) {
                volumeId = matches.item(0).getFirstChild().getNodeValue();
            }
            if( volumeId == null ) {
                matches = doc.getElementsByTagName("id"); // v2.2
                if( matches.getLength() > 0 ) {
                    volumeId = matches.item(0).getFirstChild().getNodeValue();
                }
            }
            if( volumeId == null ) {
                matches = doc.getElementsByTagName("jobid"); // v4.1
                if( matches.getLength() > 0 ) {
                    volumeId = matches.item(0).getFirstChild().getNodeValue();
                }
            }
            if( volumeId == null ) {
                throw new CloudException("Failed to create volume");
            }
            Document responseDoc = getProvider().waitForJob(doc, "Create Volume");
            if (responseDoc != null){
                NodeList nodeList = responseDoc.getElementsByTagName("volume");
                if (nodeList.getLength() > 0) {
                    Node volume = nodeList.item(0);
                    NodeList attributes = volume.getChildNodes();
                    for (int i = 0; i<attributes.getLength(); i++) {
                        Node attribute = attributes.item(i);
                        String name = attribute.getNodeName().toLowerCase();
                        String value;

                        if( attribute.getChildNodes().getLength() > 0 ) {
                            value = attribute.getFirstChild().getNodeValue();
                        }
                        else {
                            value = null;
                        }
                        if (name.equalsIgnoreCase("id")) {
                            volumeId = value;
                            break;
                        }
                    }
                }
            }
            
            // Set tags
            List<Tag> tags = new ArrayList<>();
            Map<String, Object> meta = options.getMetaData();
            for( Map.Entry<String, Object> entry : meta.entrySet() ) {
            	if( entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description") ) {
            		continue;
            	}
            	if (entry.getValue() != null && !entry.getValue().equals("")) {
            		tags.add(new Tag(entry.getKey(), entry.getValue().toString()));
            	}
            }
            tags.add(new Tag("Name", options.getName()));
            tags.add(new Tag("Description", options.getDescription()));
            getProvider().createTags(new String[]{volumeId}, "Volume", tags.toArray(new Tag[tags.size()]));
            return volumeId;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void detach(@Nonnull String volumeId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.detach");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(DETACH_VOLUME, new Param("id", volumeId));

            getProvider().waitForJob(doc, "Detach Volume");
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile CSVolumeCapabilities capabilities;
    @Override
    public VolumeCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new CSVolumeCapabilities(getProvider());
        }
        return capabilities;
    }

    @Nonnull Collection<DiskOffering> getDiskOfferings() throws InternalException, CloudException {
        final Document doc = new CSMethod(getProvider()).get(LIST_DISK_OFFERINGS);
        List<DiskOffering> offerings = new ArrayList<>();
        NodeList matches = doc.getElementsByTagName("diskoffering");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            DiskOffering offering = new DiskOffering();
            Node node = matches.item(i);
            NodeList attributes;
            
            attributes = node.getChildNodes();
            for( int j=0; j<attributes.getLength(); j++ ) {
                Node n = attributes.item(j);
                String value;

                if( n.getChildNodes().getLength() > 0 ) {
                    value = n.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( n.getNodeName().equals("id") ) {
                    offering.id = value;
                }
                else if( n.getNodeName().equals("disksize") ) {
                    offering.diskSize = Long.parseLong(value);
                }
                else if( n.getNodeName().equalsIgnoreCase("name") ) {
                    offering.name = value;
                }
                else if( n.getNodeName().equalsIgnoreCase("displayText") ) {
                    offering.description = value;
                }
                else if( n.getNodeName().equalsIgnoreCase("storagetype") ) {
                    offering.type = value;
                }
            }
            if( offering.id != null ) {
                if( offering.name == null ) {
                    if( offering.diskSize > 0 ) {
                        offering.name = offering.diskSize + " GB";
                    }
                    else {
                        offering.name = "Custom #" + offering.id;
                    }
                }
                if( offering.description == null ) {
                    offering.description = offering.name;
                }
                offerings.add(offering);
            }
        }
        return offerings;
    }
    
    @Nullable
    String getRootVolumeId(@Nonnull String serverId) throws InternalException, CloudException {
        final Volume volume = getRootVolume(serverId);
        
        return (volume == null ? null : volume.getProviderVolumeId());
    }

    private @Nullable Volume getRootVolume(@Nonnull String serverId) throws InternalException, CloudException {
        final Document doc = new CSMethod(getProvider()).get(LIST_VOLUMES, new Param("virtualMachineId", serverId));
        NodeList matches = doc.getElementsByTagName("volume");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node v = matches.item(i);

            if( v != null ) {
                Volume volume = toVolume(v, true);
                
                if( volume != null ) {
                    return volume;
                }
            }
        }
        return null;
    }
    
    @Override
    public @Nullable Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.getVolume");
        try {
            final Document doc = new CSMethod(getProvider()).get(LIST_VOLUMES, new Param("id", volumeId), new Param("zoneId", getContext().getRegionId()));
            NodeList matches = doc.getElementsByTagName("volume");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node v = matches.item(i);

                if( v != null ) {
                    return toVolume(v, false);
                }
            }
            return null;
        }
        catch( CSException e ) {
            if( e.getHttpCode() == 431 ) {
                return null;
            }
            throw e;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Volume.isSubscribed");
        try {
            return getProvider().getComputeServices().getVirtualMachineSupport().isSubscribed();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumeProducts");
        try {
            Cache<VolumeProduct> cache = Cache.getInstance(getProvider(), "volumeProducts", VolumeProduct.class, CacheLevel.REGION_ACCOUNT);
            Iterable<VolumeProduct> products = cache.get(getContext());

            if( products == null ) {
                ArrayList<VolumeProduct> list = new ArrayList<VolumeProduct>();

                for( DiskOffering offering : getDiskOfferings() ) {
                    VolumeProduct p = toProduct(offering);

                    if( p != null && (!getProvider().getServiceProvider().equals(CSServiceProvider.DEMOCLOUD) || "local".equals(offering.type)) ) {
                        list.add(p);
                    }
                }
                products = Collections.unmodifiableList(list);
                cache.put(getContext(), products);
            }
            return products;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVolumeStatus() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumeStatus");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(LIST_VOLUMES, new Param("zoneId", getContext().getRegionId()));
            List<ResourceStatus> volumes = new ArrayList<>();

            int numPages = 1;
            NodeList nodes = doc.getElementsByTagName("count");
            Node n = nodes.item(0);
            if (n != null) {
                String value = n.getFirstChild().getNodeValue().trim();
                int count = Integer.parseInt(value);
                numPages = count/500;
                int remainder = count % 500;
                if (remainder > 0) {
                    numPages++;
                }
            }

            for (int page = 1; page <= numPages; page++) {
                if (page > 1) {
                    String nextPage = String.valueOf(page);
                    doc = method.get(LIST_VOLUMES, new Param("zoneId", getContext().getRegionId()), new Param("pagesize", "500"), new Param("page", nextPage));
                }
                NodeList matches = doc.getElementsByTagName("volume");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node v = matches.item(i);

                    if( v != null ) {
                        ResourceStatus volume = toStatus(v);

                        if( volume != null ) {
                            volumes.add(volume);
                        }
                    }
                }
            }
            return volumes;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.listVolumes");
        try {
            return listVolumes(false);
        }
        finally {
            APITrace.end();
        }
    }
     
    private @Nonnull Collection<Volume> listVolumes(boolean rootOnly) throws InternalException, CloudException {
        CSMethod method = new CSMethod(getProvider());
        Document doc = method.get(LIST_VOLUMES, new Param("zoneId", getContext().getRegionId()));
        ArrayList<Volume> volumes = new ArrayList<>();
        int numPages = 1;
        NodeList nodes = doc.getElementsByTagName("count");
        Node n = nodes.item(0);
        if (n != null) {
            String value = n.getFirstChild().getNodeValue().trim();
            int count = Integer.parseInt(value);
            numPages = count/500;
            int remainder = count % 500;
            if (remainder > 0) {
                numPages++;
            }
        }

        for (int page = 1; page <= numPages; page++) {
            if (page > 1) {
                String nextPage = String.valueOf(page);
                doc = method.get(LIST_VOLUMES, new Param("zoneId", getContext().getRegionId()), new Param("pagesize", "500"), new Param("page", nextPage));
            }
            NodeList matches = doc.getElementsByTagName("volume");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node v = matches.item(i);

                if( v != null ) {
                    Volume volume = toVolume(v, rootOnly);

                    if( volume != null ) {
                        volumes.add(volume);
                    }
                }
            }
        }
        return volumes;
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Volume.remove");
        try {
            Document doc = new CSMethod(getProvider()).get(DELETE_VOLUME, new Param("id", volumeId));

            getProvider().waitForJob(doc, "Delete Volume");
        }
        finally {
            APITrace.end();
        }
    }
    
    private @Nonnull String toDeviceNumber(@Nonnull String deviceId) {
        if( !deviceId.startsWith("/dev/") && !deviceId.startsWith("hd") ) {
            deviceId = "/dev/" + deviceId;
        }
        if( deviceId.equals("/dev/xvda") ) { return "0"; }
        else if( deviceId.equals("/dev/xvdb") ) { return "1"; }
        else if( deviceId.equals("/dev/xvdc") ) { return "2"; }
        else if( deviceId.equals("/dev/xvde") ) { return "4"; }
        else if( deviceId.equals("/dev/xvdf") ) { return "5"; }
        else if( deviceId.equals("/dev/xvdg") ) { return "6"; }
        else if( deviceId.equals("/dev/xvdh") ) { return "7"; }
        else if( deviceId.equals("/dev/xvdi") ) { return "8"; }
        else if( deviceId.equals("/dev/xvdj") ) { return "9"; }
        else if( deviceId.equals("hda") ) { return "0"; }
        else if( deviceId.equals("hdb") ) { return "1"; }
        else if( deviceId.equals("hdc") ) { return "2"; }
        else if( deviceId.equals("hdd") ) { return "3"; }
        else if( deviceId.equals("hde") ) { return "4"; }
        else if( deviceId.equals("hdf") ) { return "5"; }
        else if( deviceId.equals("hdg") ) { return "6"; }
        else if( deviceId.equals("hdh") ) { return "7"; }
        else if( deviceId.equals("hdi") ) { return "8"; }
        else if( deviceId.equals("hdj") ) { return "9"; }
        else { return "9"; }
    }

    private @Nonnull String toDeviceID(@Nonnull String deviceNumber, boolean isWindows) {
        if (deviceNumber == null){
            return null;
        }
        if (!isWindows){
            if( deviceNumber.equals("0") ) { return "/dev/xvda"; }
            else if( deviceNumber.equals("1") ) { return "/dev/xvdb"; }
            else if( deviceNumber.equals("2") ) { return "/dev/xvdc"; }
            else if( deviceNumber.equals("4") ) { return "/dev/xvde"; }
            else if( deviceNumber.equals("5") ) { return "/dev/xvdf"; }
            else if( deviceNumber.equals("6") ) { return "/dev/xvdg"; }
            else if( deviceNumber.equals("7") ) { return "/dev/xvdh"; }
            else if( deviceNumber.equals("8") ) { return "/dev/xvdi"; }
            else if( deviceNumber.equals("9") ) { return "/dev/xvdj"; }
            else { return "/dev/xvdj"; }
        }
        else{
            if( deviceNumber.equals("0") ) { return "hda"; }
            else if( deviceNumber.equals("1") ) { return "hdb"; }
            else if( deviceNumber.equals("2") ) { return "hdc"; }
            else if( deviceNumber.equals("3") ) { return "hdd"; }
            else if( deviceNumber.equals("4") ) { return "hde"; }
            else if( deviceNumber.equals("5") ) { return "hdf"; }
            else if( deviceNumber.equals("6") ) { return "hdg"; }
            else if( deviceNumber.equals("7") ) { return "hdh"; }
            else if( deviceNumber.equals("8") ) { return "hdi"; }
            else if( deviceNumber.equals("9") ) { return "hdj"; }
            else { return "hdj"; }
        }
    }

    private @Nullable VolumeProduct toProduct(@Nullable DiskOffering offering) throws InternalException, CloudException {
        if( offering == null ) {
            return null;
        }
        if( offering.diskSize < 1 ) {
            return VolumeProduct.getInstance(offering.id, offering.name, offering.description, VolumeType.HDD);
        }
        else {
            return VolumeProduct.getInstance(offering.id, offering.name, offering.description, VolumeType.HDD, new Storage<>(offering.diskSize, Storage.GIGABYTE));
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws InternalException, CloudException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        VolumeState volumeState = null;
        String volumeId = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute != null ) {
                String name = attribute.getNodeName();

                if( name.equals("id") ) {
                    volumeId = attribute.getFirstChild().getNodeValue().trim();
                }
                else if( name.equals("state") && attribute.hasChildNodes() ) {
                    String state = attribute.getFirstChild().getNodeValue();

                    if( state == null ) {
                        volumeState = VolumeState.PENDING;
                    }
                    else if( state.equalsIgnoreCase("created") || state.equalsIgnoreCase("ready") || state.equalsIgnoreCase("allocated") ) {
                        volumeState = VolumeState.AVAILABLE;
                    }
                    else {
                        logger.warn("DEBUG: Unknown state for CloudStack volume: " + state);
                        volumeState = VolumeState.PENDING;
                    }
                }
                if( volumeId != null && volumeState != null ) {
                    break;
                }
            }
        }
        if( volumeId == null ) {
            return null;
        }
        if( volumeState == null ) {
            volumeState = VolumeState.PENDING;
        }
        return new ResourceStatus(volumeId, volumeState);
    }

    private @Nullable Volume toVolume(@Nullable Node node, boolean rootOnly) throws InternalException, CloudException {
        if( node == null ) {
            return null;
        }
        
        Volume volume = new Volume();
        String offeringId = null;
        NodeList attributes = node.getChildNodes();
        String volumeName = null, description = null;
        boolean root = false;
        String deviceNumber = null;

        volume.setFormat(VolumeFormat.BLOCK);
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            
            if( attribute != null ) {
                String name = attribute.getNodeName();
                
                if( name.equals("id") ) {
                    volume.setProviderVolumeId(attribute.getFirstChild().getNodeValue().trim());
                }
                if( name.equals("zoneid") ) {
                    String zid = attribute.getFirstChild().getNodeValue().trim();
                    if( !getContext().getRegionId().equals(zid) ) {
                        System.out.println("Zone mismatch: " + getContext().getRegionId());
                        System.out.println("               " + zid);
                        return null;
                    }
                }
                else if( name.equals("type") && attribute.hasChildNodes() ) {
                    if( attribute.getFirstChild().getNodeValue().equalsIgnoreCase("root") ) {
                        root = true;
                    }
                }
                else if( name.equals("diskofferingid") && attribute.hasChildNodes() ) {
                    offeringId = attribute.getFirstChild().getNodeValue().trim();
                }
                else if( name.equals("name") && attribute.hasChildNodes() ) {
                    volumeName = attribute.getFirstChild().getNodeValue().trim();
                }
                else if ( name.equals("deviceid") && attribute.hasChildNodes()){
                    deviceNumber = attribute.getFirstChild().getNodeValue().trim();
                }
                else if( name.equalsIgnoreCase("virtualmachineid") && attribute.hasChildNodes() ) {
                    volume.setProviderVirtualMachineId(attribute.getFirstChild().getNodeValue());
                }
                else if( name.equals("displayname") && attribute.hasChildNodes() ) {
                    description = attribute.getFirstChild().getNodeValue().trim();
                }
                else if( name.equals("size") && attribute.hasChildNodes() ) {
                    long size = (Long.parseLong(attribute.getFirstChild().getNodeValue())/1024000000L);

                    volume.setSize(new Storage<Gigabyte>(size, Storage.GIGABYTE));
                }
                else if( name.equals("state") && attribute.hasChildNodes() ) {
                    String state = attribute.getFirstChild().getNodeValue();

                    if( state == null ) {
                        volume.setCurrentState(VolumeState.PENDING);
                    }
                    else if( state.equalsIgnoreCase("created") || state.equalsIgnoreCase("ready")
                            || state.equalsIgnoreCase("allocated") || state.equalsIgnoreCase("uploaded")) {
                        volume.setCurrentState(VolumeState.AVAILABLE);
                    }
                    else {
                        logger.warn("DEBUG: Unknown state for CloudStack volume: " + state);
                        volume.setCurrentState(VolumeState.PENDING);
                    }
                }
                else if( name.equals("created") && attribute.hasChildNodes() ) {
                    String date = attribute.getFirstChild().getNodeValue();
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); //2009-02-03T05:26:32.612278
                    
                    try {
                        volume.setCreationTimestamp(df.parse(date).getTime());
                    }
                    catch( ParseException e ) {
                        volume.setCreationTimestamp(0L);
                    }
                }
            }
        }
        if( !root && rootOnly ) {
            return null;
        }
        if( volume.getProviderVolumeId() == null ) {
            return null;
        }
        if( volumeName == null ) {
            volume.setName(volume.getProviderVolumeId());
        }
        else {
            volume.setName(volumeName);
        }
        if( description == null ) {
            volume.setDescription(volume.getName());
        }
        else {
            volume.setDescription(description);
        }
        if( offeringId != null ) {
            volume.setProviderProductId(offeringId);
        }
        volume.setProviderRegionId(getContext().getRegionId());
        volume.setProviderDataCenterId(getContext().getRegionId());

        volume.setDeviceId(deviceNumber);
        volume.setRootVolume(root);
        volume.setType(VolumeType.HDD);
        if( root ) {
            volume.setGuestOperatingSystem(Platform.guess(volume.getName() + " " + volume.getDescription()));
        }
        return volume;
    }
    
    @Override
    public void setTags(@Nonnull String volumeId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	setTags(new String[] { volumeId }, tags);
    }
    
    @Override
    public void setTags(@Nonnull String[] volumeIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Volume.setTags");
    	try {
    		removeTags(volumeIds);
    		getProvider().createTags(volumeIds, "Volume", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void updateTags(@Nonnull String volumeId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	updateTags(new String[] { volumeId }, tags);
    }
    
    @Override
    public void updateTags(@Nonnull String[] volumeIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Volume.updateTags");
    	try {
    		getProvider().updateTags(volumeIds, "Volume", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void removeTags(@Nonnull String volumeId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	removeTags(new String[] { volumeId }, tags);
    }
    
    @Override
    public void removeTags(@Nonnull String[] volumeIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Volume.removeTags");
    	try {
    		getProvider().removeTags(volumeIds, "Volume", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
}