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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.cloudstack.CloudstackException;
import org.dasein.cloud.cloudstack.CloudstackMethod;
import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.cloudstack.Zones;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Volumes implements VolumeSupport {
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
        
        public String toString() {return "DiskOffering ["+id+"] of size "+diskSize;}
        
    }
    
    private CloudstackProvider provider;
    
    Volumes(CloudstackProvider provider) {
        this.provider = provider;
    }
    
    @Override
    public void attach(String volumeId, String serverId, String deviceId) throws InternalException, CloudException {
        Param[] params;
        
        if( logger.isInfoEnabled() ) {
            logger.info("attaching " + volumeId + " to " + serverId + " as " + deviceId);
        }
        if( deviceId == null ) {
            params = new Param[] { new Param("id", volumeId), new Param("virtualMachineId", serverId) }; 
        }
        else {
            deviceId = toDeviceNumber(deviceId);
            if( logger.isDebugEnabled() ) {
                logger.debug("Device mapping is: " + deviceId);
            }
            params = new Param[] { new Param("id", volumeId), new Param("virtualMachineId", serverId), new Param("deviceId", deviceId) };
        }
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(ATTACH_VOLUME, params));

        if( doc == null ) {
            throw new CloudException("No such volume or server");
        }
        provider.waitForJob(doc, "Attach Volume");
    }

    @Override
    public String create(String snapshotId, int size, String zoneId) throws InternalException, CloudException {
        String name = "vol_" + System.currentTimeMillis();
        DiskOffering bestOffering = null;
        
        long requestedSize = 0;
        
        if( snapshotId == null ) {
            for( DiskOffering offering : getDiskOfferings() ) {
            	if (offering.diskSize > 100000) {  // This cloud isn't returning sizes in GB... must be Bytes.
            		requestedSize = size * 1024 * 1024 * 1024;	
            	} else {
            		requestedSize = size;
            	}
                if( offering.diskSize == requestedSize ) {
                    bestOffering = offering;
                    break;
                }
                else if( bestOffering == null ) {
                    bestOffering = offering;
                }
                else if( bestOffering.diskSize < requestedSize ) {
                    if( offering.diskSize > requestedSize || offering.diskSize > bestOffering.diskSize ) {
                        bestOffering = offering;
                    }
                }
                else if( offering.diskSize > requestedSize && offering.diskSize < bestOffering.diskSize ) {
                    bestOffering = offering;
                }
            }
            if( bestOffering == null ) {
                throw new CloudException("No matching volume size offering for " + requestedSize);
            }
        }
        Param[] params = new Param[3];

        params[0] = new Param("name", name);
        params[1] = new Param("zoneId", provider.getContext().getRegionId());
        if( snapshotId != null ) {
            params[2] = new Param("snapshotId", snapshotId);
        }
        else {
            params[2] = new Param("diskOfferingId", bestOffering.id);
        }
        
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(CREATE_VOLUME, params));
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
            throw new CloudException("Failed to create volume");
        }
        provider.waitForJob(doc, "Create Volume");
        return volumeId;
    }

    @Override
    public void detach(String volumeId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(DETACH_VOLUME, new Param[] { new Param("id", volumeId) }));

        provider.waitForJob(doc, "Detach Volume");
    }
    
    String getDiskOfferingId(String forServerId) throws InternalException, CloudException {
        VolumeConfig it = null;

        for( VolumeConfig cfg : listVolumeConfigs(forServerId) ) {
            if( !cfg.root ) {
                it = cfg;
                break;
            }
        }
        return (it == null ? null : it.offeringId);
    }
    
    Collection<DiskOffering> getDiskOfferings() throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_DISK_OFFERINGS, new Param[0]));
        ArrayList<DiskOffering> offerings = new ArrayList<DiskOffering>();
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
                if( offering.id != null && offering.diskSize > 0 ) {
                    break;
                }
            }
            if( offering.id != null ) {
                offerings.add(offering);
            }
        }
        return offerings;
    }
    
    @Override
    public String getProviderTermForVolume(Locale locale) {
        return "volume";
    }

    String getRootVolumeId(String serverId) throws InternalException, CloudException {
        VolumeConfig cfg = getRootVolume(serverId);
        
        return ((cfg == null || cfg.volume == null) ? null : cfg.volume.getProviderVolumeId());
    }
    
    String getRootVolumeOffering(String serverId) throws InternalException, CloudException {
        return getRootVolume(serverId).offeringId;
    }
    
    private VolumeConfig getRootVolume(String serverId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_VOLUMES, new Param[] { new Param("virtualMachineId", serverId) }));        
        NodeList matches = doc.getElementsByTagName("volume");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node v = matches.item(i);

            if( v != null ) {
                VolumeConfig cfg = toVolume(v, true);
                
                if( cfg != null ) {
                    return cfg;
                }
            }
        }
        return null;
    }
    
    @Override
    public Volume getVolume(String volumeId) throws InternalException, CloudException {
        VolumeConfig cfg = getVolumeConfig(volumeId);
        
        return (cfg == null ? null : cfg.volume);
    }

    private VolumeConfig getVolumeConfig(String volumeId) throws InternalException, CloudException {
        try {
            CloudstackMethod method = new CloudstackMethod(provider);
            Document doc = method.get(method.buildUrl(LIST_VOLUMES, new Param[] { new Param("id", volumeId), new Param("zoneId", provider.getContext().getRegionId()) }));
            NodeList matches = doc.getElementsByTagName("volume");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node v = matches.item(i);

                if( v != null ) {
                    return toVolume(v, false);
                }
            }
            return null;
        }
        catch( CloudstackException e ) {
            if( e.getHttpCode() == 431 ) {
                return null;
            }
            throw e;
        }
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    private List<String> unixDeviceIdList    = null;
    private List<String> windowsDeviceIdList = null;
    
    @Override
    public Iterable<String> listPossibleDeviceIds(Platform platform) throws InternalException, CloudException {
        if( platform.isWindows() ) {
            if( windowsDeviceIdList == null ) {
                ArrayList<String> list = new ArrayList<String>();
                
                list.add("hde");
                list.add("hdf");
                list.add("hdg");
                list.add("hdh");
                list.add("hdi");
                list.add("hdj");
                windowsDeviceIdList = Collections.unmodifiableList(list);
            }
            return windowsDeviceIdList;            
        }
        else {
            if( unixDeviceIdList == null ) {
                ArrayList<String> list = new ArrayList<String>();
                
                list.add("/dev/xvdc");
                list.add("/dev/xvde");
                list.add("/dev/xvdf");
                list.add("/dev/xvdg");
                list.add("/dev/xvdh");
                list.add("/dev/xvdi");
                list.add("/dev/xvdj");
                unixDeviceIdList = Collections.unmodifiableList(list);
            }
            return unixDeviceIdList;
        }
    }
    
    @Override
    public Iterable<Volume> listVolumes() throws InternalException, CloudException {
        return listVolumes(false);
    }
     
    private Collection<Volume> listVolumes(boolean rootOnly) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_VOLUMES, new Param[] { new Param("zoneId", provider.getContext().getRegionId()) }));
        ArrayList<Volume> volumes = new ArrayList<Volume>();
        NodeList matches = doc.getElementsByTagName("volume");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node v = matches.item(i);

            if( v != null ) {
                VolumeConfig volume = toVolume(v, rootOnly);
                
                if( volume != null && volume.volume != null ) {
                    volumes.add(volume.volume);
                }
            }
        }
        return volumes;
    }
    
    public Iterable<Volume> listVolumesAttachedTo(final String serverId) throws InternalException, CloudException {
        provider.hold();
        
        PopulatorThread<Volume> populator = new PopulatorThread<Volume>(new JiteratorPopulator<Volume>() {
            @Override
            public void populate(Jiterator<Volume> it) throws Exception {
                try {
                    for( VolumeConfig volume : listVolumeConfigs(serverId) ) {
                        it.push(volume.volume);
                    }
                }
                finally {
                    provider.release();
                }
            }            
        });
        
        populator.populate();
        return populator.getResult();
    }
    
    private Collection<VolumeConfig> listVolumeConfigs(String serverId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_VOLUMES, new Param[] { new Param("virtualMachineId", serverId), new Param("zoneId", provider.getContext().getRegionId()) }));
        ArrayList<VolumeConfig> volumes = new ArrayList<VolumeConfig>();
        NodeList matches = doc.getElementsByTagName("volume");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node v = matches.item(i);

            if( v != null ) {
                VolumeConfig volume = toVolume(v, false);
                
                if( volume != null && volume.volume != null ) {
                    volumes.add(volume);
                }
            }
        }
        return volumes;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(String volumeId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(DELETE_VOLUME, new Param[] { new Param("id", volumeId) }));

        provider.waitForJob(doc, "Delete Volume");
    }
    
    private String toDeviceNumber(String deviceId) {
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
    
    static private class VolumeConfig {
        public VolumeConfig() { }
        
        public Volume volume;
        public String offeringId;
        public boolean root;
    }
    
    private VolumeConfig toVolume(Node node, boolean rootOnly) throws InternalException, CloudException {
        if( node == null ) {
            return null;
        }
        
        Volume volume = new Volume();
        String offeringId = null;
        NodeList attributes = node.getChildNodes();
        boolean root = false;
        
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            
            if( attribute != null ) {
                String name = attribute.getNodeName();
                
                if( name.equals("id") ) {
                    volume.setProviderVolumeId(attribute.getFirstChild().getNodeValue().trim());
                }
                if( name.equals("zoneid") ) {
                    String zid = attribute.getFirstChild().getNodeValue().trim();
                    if( !provider.getContext().getRegionId().equals(zid) ) {
                        System.out.println("Zone mismatch: " + provider.getContext().getRegionId());
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
                    if( volume.getName() == null ) {
                        volume.setName(attribute.getFirstChild().getNodeValue());
                    }
                }
                else if( name.equalsIgnoreCase("virtualmachineid") && attribute.hasChildNodes() ) {
                    volume.setProviderVirtualMachineId(attribute.getFirstChild().getNodeValue());
                }
                else if( name.equals("displayname") && attribute.hasChildNodes() ) {
                    volume.setName(attribute.getFirstChild().getNodeValue());
                }
                else if( name.equals("size") && attribute.hasChildNodes() ) {
                    volume.setSizeInGigabytes((int)(Long.parseLong(attribute.getFirstChild().getNodeValue())/1024000000L));
                }
                else if( name.equals("state") && attribute.hasChildNodes() ) {
                    String state = attribute.getFirstChild().getNodeValue();

                    if( state == null ) {
                        volume.setCurrentState(VolumeState.PENDING);
                    }
                    else if( state.equalsIgnoreCase("created") || state.equalsIgnoreCase("allocated") || state.equalsIgnoreCase("ready") ) {
                        volume.setCurrentState(VolumeState.AVAILABLE);
                    }
                    else {
                        logger.warn("Unknown state for CloudStack volume: " + state);
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
        String name = volume.getName();
        
        if( name == null ) {
            name = volume.getProviderVolumeId();
            volume.setName(name);
        }
        volume.setProviderRegionId(provider.getContext().getRegionId());
        volume.setProviderDataCenterId(provider.getContext().getRegionId());
        if( volume.getProviderVirtualMachineId() != null ) {
            if( root ) {
                volume.setDeviceId("/dev/xvda2");
            }
        }
        VolumeConfig v = new VolumeConfig();
        
        v.volume = volume;
        v.offeringId = offeringId;
        v.root = root;
        return v;
    }
}
