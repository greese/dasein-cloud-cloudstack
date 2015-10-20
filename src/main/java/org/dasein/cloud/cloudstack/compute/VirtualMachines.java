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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.CSTopology;
import org.dasein.cloud.cloudstack.CSVersion;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.cloudstack.network.Network;
import org.dasein.cloud.cloudstack.network.SecurityGroup;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Hour;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class VirtualMachines extends AbstractVMSupport<CSCloud> {
    static public final Logger logger = Logger.getLogger(VirtualMachines.class);
    
    static private final String DEPLOY_VIRTUAL_MACHINE  = "deployVirtualMachine";
    static private final String DESTROY_VIRTUAL_MACHINE = "destroyVirtualMachine";
    static private final String GET_VIRTUAL_MACHINE_PASSWORD = "getVMPassword";
    static private final String LIST_VIRTUAL_MACHINES   = "listVirtualMachines";
    static private final String LIST_SERVICE_OFFERINGS  = "listServiceOfferings";
    static private final String REBOOT_VIRTUAL_MACHINE  = "rebootVirtualMachine";
    static private final String RESET_VIRTUAL_MACHINE_PASSWORD = "resetPasswordForVirtualMachine";
    static private final String RESIZE_VIRTUAL_MACHINE  = "scaleVirtualMachine";
    static private final String START_VIRTUAL_MACHINE   = "startVirtualMachine";
    static private final String STOP_VIRTUAL_MACHINE    = "stopVirtualMachine";
    
    static private Properties                              cloudMappings;
    static private Map<String,Map<String,String>>          customNetworkMappings;
    static private Map<String,Map<String,Set<String>>>     customServiceMappings;
    
    public VirtualMachines(CSCloud provider) {
        super(provider);
    }

    @Override
    public VirtualMachine alterVirtualMachineSize( @Nonnull String vmId, @Nullable String cpuCount, @Nullable String ramInMB ) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.alterVirtualMachineSize");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            boolean restart = false;
            if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                restart = true;
                stop(vmId, true);
            }

            long timeout = System.currentTimeMillis()+(CalendarWrapper.MINUTE*20L);
            while (System.currentTimeMillis() < timeout) {
                if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                    try {
                        Thread.sleep(15000L);
                        vm = getVirtualMachine(vmId);
                    }
                    catch (InterruptedException ignore) {}
                }
                else {
                    break;
                }
            }
            vm = getVirtualMachine(vmId);
            if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                throw new CloudException("Unable to stop vm for scaling");
            }
            List<Param> params = new ArrayList<Param>();
            params.add(new Param("id", vmId));
            params.add(new Param("serviceOfferingId", vm.getProductId()));
            int index = 0;
            if( cpuCount != null ) {
                params.add(new Param("details["+index+"].cpunumber", cpuCount));
                index++;
            }
            if( ramInMB != null ) {
                params.add(new Param("details["+index+"].memory", ramInMB));
            }
            Document doc = new CSMethod(getProvider()).get(RESIZE_VIRTUAL_MACHINE, params);

            NodeList matches = doc.getElementsByTagName("scalevirtualmachineresponse");
            String jobId = null;

            for( int i=0; i<matches.getLength(); i++ ) {
                NodeList attrs = matches.item(i).getChildNodes();

                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node node = attrs.item(j);

                    if (node != null && node.getNodeName().equalsIgnoreCase("jobid") ) {
                        jobId = node.getFirstChild().getNodeValue();
                    }
                }
            }
            if( jobId == null ) {
                throw new CloudException("Could not scale server");
            }
            Document responseDoc = getProvider().waitForJob(doc, "Scale Server");

            if (responseDoc != null){
                NodeList nodeList = responseDoc.getElementsByTagName("virtualmachine");
                if (nodeList.getLength() > 0) {
                    Node virtualMachine = nodeList.item(0);
                    vm = toVirtualMachine(virtualMachine);
                    if( vm != null ) {
                        if (restart) {
                            start(vmId);
                        }
                        return vm;
                    }
                }
            }
            if (restart) {
                start(vmId);
            }
            return getVirtualMachine(vmId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public VirtualMachine alterVirtualMachineProduct(@Nonnull String vmId, @Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.alterVirtualMachineProduct");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);
            if (vm.getProductId().equals(productId)) {
                return vm;
            }

            boolean restart = false;
            if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                restart = true;
                stop(vmId, true);
            }

            long timeout = System.currentTimeMillis()+(CalendarWrapper.MINUTE*20L);
            while (System.currentTimeMillis() < timeout) {
                if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                    try {
                        Thread.sleep(15000L);
                        vm = getVirtualMachine(vmId);
                    }
                    catch (InterruptedException ignore) {}
                }
                else {
                    break;
                }
            }
            vm = getVirtualMachine(vmId);
            if (!vm.getCurrentState().equals(VmState.STOPPED)) {
                throw new CloudException("Unable to stop vm for scaling");
            }
            Document doc = new CSMethod(getProvider()).get(RESIZE_VIRTUAL_MACHINE,
                    new Param("id", vmId),
                    new Param("serviceOfferingId", productId));

            NodeList matches = doc.getElementsByTagName("scalevirtualmachineresponse");
            String jobId = null;

            for( int i=0; i<matches.getLength(); i++ ) {
                NodeList attrs = matches.item(i).getChildNodes();

                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node node = attrs.item(j);

                    if (node != null && node.getNodeName().equalsIgnoreCase("jobid") ) {
                        jobId = node.getFirstChild().getNodeValue();
                    }
                }
            }
            if( jobId == null ) {
                throw new CloudException("Could not scale server");
            }
            Document responseDoc = getProvider().waitForJob(doc, "Scale Server");

            if (responseDoc != null){
                NodeList nodeList = responseDoc.getElementsByTagName("virtualmachine");
                if (nodeList.getLength() > 0) {
                    Node virtualMachine = nodeList.item(0);
                    vm = toVirtualMachine(virtualMachine);
                    if( vm != null ) {
                        if (restart) {
                            start(vmId);
                        }
                        return vm;
                    }
                }
            }
            if (restart) {
                start(vmId);
            }
            return getVirtualMachine(vmId);
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile VMCapabilities capabilities;
    @Nonnull
    @Override
    public VirtualMachineCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new VMCapabilities(getProvider());
        }
        return capabilities;
    }

    @Nullable
    @Override
    public VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
        return VMScalingCapabilities.getInstance(false,true,Requirement.NONE,Requirement.NONE);
    }

    @Nullable
    @Override
    public String getPassword(@Nonnull String vmId) throws InternalException, CloudException {
        return getRootPassword(vmId);
    }

    private String getRootPassword(@Nonnull String serverId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.getPassword");
        try {
            Document doc = new CSMethod(getProvider()).get(GET_VIRTUAL_MACHINE_PASSWORD, new Param("id", serverId));

            if (doc != null){
                NodeList matches = doc.getElementsByTagName("getvmpasswordresponse");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        NodeList attributes = node.getChildNodes();
                        for( int j=0; j<attributes.getLength(); j++ ) {
                            Node attribute = attributes.item(j);
                            String name = attribute.getNodeName().toLowerCase();
                            String value;

                            if( attribute.getChildNodes().getLength() > 0 ) {
                                value = attribute.getFirstChild().getNodeValue();
                            }
                            else {
                                value = null;
                            }
                            if( name.equals("password") ) {
                                NodeList nodes = attribute.getChildNodes();
                                for( int k=0; k<nodes.getLength(); k++ ) {
                                    Node password = nodes.item(k);
                                    name = password.getNodeName().toLowerCase();

                                    if( password.getChildNodes().getLength() > 0 ) {
                                        value = password.getFirstChild().getNodeValue();
                                    }
                                    else {
                                        value = null;
                                    }
                                    if( name.equals("encryptedpassword") ) {
                                        return value;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            logger.warn("Unable to find password for vm with id "+serverId);
            return null;
        }
        catch (CSException e) {
            if (e.getHttpCode() == 431) {
                logger.warn("No password found for vm "+serverId);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.getVirtualMachine");
        try {
            Document doc = new CSMethod(getProvider()).get(LIST_VIRTUAL_MACHINES, new Param("id", serverId));
            NodeList matches = doc.getElementsByTagName("virtualmachine");

            if( matches.getLength() < 1 ) {
                return null;
            }
            for( int i=0; i<matches.getLength(); i++ ) {
                VirtualMachine s = toVirtualMachine(matches.item(i));

                if( s != null && s.getProviderVirtualMachineId().equals(serverId) ) {
                    return s;
                }
            }
            return null;
        }
        catch( CloudException e ) {
            if( e.getMessage().contains("does not exist") ) {
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
        APITrace.begin(getProvider(), "VM.isSubscribed");
        try {
            new CSMethod(getProvider()).get(CSTopology.LIST_ZONES, new Param("available", "true"));
            return true;
        }
        catch( CSException e ) {
            int code = e.getHttpCode();

            if( code == HttpServletResponse.SC_FORBIDDEN || code == 401 || code == 531 ) {
                return false;
            }
            throw e;
        }
        catch( CloudException e ) {
            int code = e.getHttpCode();

            if( code == HttpServletResponse.SC_FORBIDDEN || code == HttpServletResponse.SC_UNAUTHORIZED ) {
                return false;
            }
            throw e;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.launch");
        try {
            String id = withLaunchOptions.getStandardProductId();

            VirtualMachineProduct product = getProduct(id);
            if( product == null ) {
                throw new CloudException("Invalid product ID: " + id);
            }

            VirtualMachine vm ;
            if( getProvider().getVersion().greaterThan(CSVersion.CS21) ) {
                vm = launch22(withLaunchOptions.getMachineImageId(), product,  withLaunchOptions.getDataCenterId(), withLaunchOptions.getFriendlyName(), withLaunchOptions.getBootstrapKey(), withLaunchOptions.getVlanId(), withLaunchOptions.getFirewallIds(), withLaunchOptions.getUserData());
            }
            else {
                vm = launch21(withLaunchOptions.getMachineImageId(), product, withLaunchOptions.getDataCenterId(), withLaunchOptions.getFriendlyName());
            }

            // Set tags
            List<Tag> tags = new ArrayList<Tag>();
            Map<String, Object> meta = withLaunchOptions.getMetaData();
            for( Map.Entry<String, Object> entry : meta.entrySet() ) {
            	if( entry.getKey().equalsIgnoreCase("name") || entry.getKey().equalsIgnoreCase("description") ) {
            		continue;
            	}
            	if (entry.getValue() != null && !entry.getValue().equals("")) {
            		tags.add(new Tag(entry.getKey(), entry.getValue().toString()));
            	}
            }
            tags.add(new Tag("Name", withLaunchOptions.getFriendlyName()));
            tags.add(new Tag("Description", withLaunchOptions.getDescription()));
            if( withLaunchOptions.getVirtualMachineGroup() != null ) {
            	tags.add(new Tag("dsnVMGroup", withLaunchOptions.getVirtualMachineGroup()));
            }
            getProvider().createTags(new String[] { vm.getProviderVirtualMachineId() }, "UserVm", tags.toArray(new Tag[tags.size()]));
            return vm;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public @Nonnull VirtualMachine launch(@Nonnull String imageId, @Nonnull VirtualMachineProduct product, @Nonnull String inZoneId, @Nonnull String name, @Nonnull String description, @Nullable String usingKey, @Nullable String withVlanId, boolean withMonitoring, boolean asSandbox, @Nullable String[] protectedByFirewalls, @Nullable Tag ... tags) throws InternalException, CloudException {
        if( getProvider().getVersion().greaterThan(CSVersion.CS21) ) {
            StringBuilder userData = new StringBuilder();
            
            if( tags != null && tags.length > 0 ) {
                for( Tag tag : tags ) {
                    userData.append(tag.getKey());
                    userData.append("=");
                    userData.append(tag.getValue());
                    userData.append("\n");
                }
            }
            else {
                userData.append("created=Dasein Cloud\n");
            }
            return launch22(imageId, product, inZoneId, name, usingKey, withVlanId, protectedByFirewalls, userData.toString());
        }
        else {
            return launch21(imageId, product, inZoneId, name);
        }
    }
    
    private VirtualMachine launch21(String imageId, VirtualMachineProduct product, String inZoneId, String name) throws InternalException, CloudException {
        return launch(new CSMethod(getProvider()).get(
                DEPLOY_VIRTUAL_MACHINE,
                new Param("zoneId", getContext().getRegionId()),
                new Param("serviceOfferingId", product.getProviderProductId()),
                new Param("templateId", imageId), new Param("displayName", name))
        );
    }
    
    private void load() {
        try {
            InputStream input = VirtualMachines.class.getResourceAsStream("/cloudMappings.cfg");
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            Properties properties = new Properties();
            String line;
            
            while( (line = reader.readLine()) != null ) {
                if( line.startsWith("#") ) {
                    continue;
                }
                int idx = line.indexOf('=');
                if( idx < 0 || line.endsWith("=") ) {
                    continue;
                }
                String cloudUrl = line.substring(0, idx);
                String cloudId = line.substring(idx+1);
                properties.put(cloudUrl, cloudId);
            }
            cloudMappings = properties;
        }
        catch( Throwable ignore ) {
            // ignore
        }        
        try {
            InputStream input = VirtualMachines.class.getResourceAsStream("/customNetworkMappings.cfg");
            Map<String,Map<String,String>> mapping = new HashMap<String,Map<String,String>>();
            Properties properties = new Properties();
            
            properties.load(input);
            for( Object key : properties.keySet() ) {
                String[] trueKey = ((String)key).split(",");
                Map<String,String> current = mapping.get(trueKey[0]);
                
                if( current == null ) {
                    current = new HashMap<String,String>();
                    mapping.put(trueKey[0], current);
                }
                current.put(trueKey[1], (String)properties.get(key));
            }
            customNetworkMappings = mapping;
        }
        catch( Throwable ignore ) {
            // ignore
        }
        try {
            InputStream input = VirtualMachines.class.getResourceAsStream("/customServiceMappings.cfg");
            Map<String,Map<String,Set<String>>> mapping = new HashMap<String,Map<String,Set<String>>>();
            Properties properties = new Properties();
            
            properties.load(input);
            
            for( Object key : properties.keySet() ) {
                String value = (String)properties.get(key);
                
                if( value != null ) {
                    String[] trueKey = ((String)key).split(",");
                    Map<String,Set<String>> tmp = mapping.get(trueKey[0]);
                    
                    if( tmp == null ) {
                        tmp =new HashMap<String,Set<String>>();
                        mapping.put(trueKey[0], tmp);
                    }
                    TreeSet<String> m = new TreeSet<String>();
                    String[] offerings = value.split(",");
                    
                    if( offerings == null || offerings.length < 1 ) {
                        m.add(value);
                    }
                    else {
                        Collections.addAll(m, offerings);
                    }
                    tmp.put(trueKey[1], m);
                }
            }
            customServiceMappings = mapping;
        }
        catch( Throwable ignore ) {
            // ignore
        }
    }
    
    private @Nonnull VirtualMachine launch22(@Nonnull String imageId, @Nonnull VirtualMachineProduct product, @Nullable String inZoneId, @Nonnull String name, @Nullable String withKeypair, @Nullable String targetVlanId, @Nullable String[] protectedByFirewalls, @Nullable String userData) throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        List<String> vlans = null;

        if( ctx == null ) {
            throw new InternalException("No context was provided for this request");
        }
        String regionId = ctx.getRegionId();

        if( inZoneId == null || inZoneId.isEmpty() ) {
            inZoneId = regionId;
        }

        if( inZoneId == null || inZoneId.isEmpty() ) {
            throw new InternalException("No region is established for this request");
        }

       String prdId = product.getProviderProductId();

        if( customNetworkMappings == null ) {
            load();
        }
        if( customNetworkMappings != null ) {
            String cloudId = cloudMappings.getProperty(ctx.getCloud().getEndpoint());
            
            if( cloudId != null ) {
                Map<String,String> map = customNetworkMappings.get(cloudId);
                
                if( map != null ) {
                    String id = map.get(prdId);
                    
                    if( id != null ) {
                        targetVlanId = id;
                    }
                }
            }
        }
        if( targetVlanId != null && targetVlanId.length() < 1 ) {
            targetVlanId = null;
        }
        if( userData == null ) {
            userData = "";
        }

        String securityGroupIds = StringUtils.join(protectedByFirewalls, ",");

        if( targetVlanId == null ) {
            Network vlan = getProvider().getNetworkServices().getVlanSupport();
            
            if( vlan != null && vlan.isSubscribed() ) {
                if( getCapabilities().identifyVlanRequirement().equals(Requirement.REQUIRED) ) {
                    vlans = vlan.findFreeNetworks();
                }
            }
        }
        else {
            vlans = new ArrayList<String>();
            vlans.add(targetVlanId);
        }
        if( securityGroupIds != null && !securityGroupIds.isEmpty() ) {
            // TODO: shouldn't we throw OpNotSupported if firewalls aren't supported but still requested?
            // otherwise it's like a confusion, no?
            if (!getProvider().getDataCenterServices().supportsSecurityGroups(regionId, vlans == null || vlans.isEmpty())) {
                securityGroupIds = null;
            }
        }
        else if( getProvider().getDataCenterServices().supportsSecurityGroups(regionId, vlans == null || vlans.isEmpty()) ) {
            /*
            String sgId = null;
            
            if( withVlanId == null ) {
                Collection<Firewall> firewalls = getProvider().getNetworkServices().getFirewallSupport().list();
                
                for( Firewall fw : firewalls ) {
                    if( fw.getName().equalsIgnoreCase("default") && fw.getProviderVlanId() == null ) {
                        sgId = fw.getProviderFirewallId();
                        break;
                    }
                }
                if( sgId == null ) {
                    try {
                        sgId = getProvider().getNetworkServices().getFirewallSupport().create("default", "Default security group");
                    }
                    catch( Throwable t ) {
                        logger.warn("Unable to create a default security group, gonna try anyways: " + t.getMessage());
                    }
                }
                if( sgId != null ) {
                    securityGroupIds = sgId;
                }
            }
            else {
                Collection<Firewall> firewalls = getProvider().getNetworkServices().getFirewallSupport().list();
                
                for( Firewall fw : firewalls ) {
                    if( (fw.getName().equalsIgnoreCase("default") || fw.getName().equalsIgnoreCase("default-" + withVlanId)) && withVlanId.equals(fw.getProviderVlanId()) ) {
                        sgId = fw.getProviderFirewallId();
                        break;
                    }
                }
                if( sgId == null ) {
                    try {
                        sgId = getProvider().getNetworkServices().getFirewallSupport().createInVLAN("default-" + withVlanId, "Default " + withVlanId + " security group", withVlanId);
                    }
                    catch( Throwable t ) {
                        logger.warn("Unable to create a default security group, gonna try anyways: " + t.getMessage());
                    }
                }
            }
            if( sgId != null ) {
                securityGroupIds = sgId;
                count++;
            }    
            */            
        }
        List<Param> params = new ArrayList<Param>();
        params.add(new Param("zoneId", inZoneId));
        params.add(new Param("serviceOfferingId", prdId));
        params.add(new Param("templateId", imageId));
        params.add(new Param("displayName", name));
        if( userData != null && userData.length() > 0 ) {
            try {
                params.add(new Param("userdata", new String(Base64.encodeBase64(userData.getBytes("utf-8")), "utf-8")));
            }
            catch( UnsupportedEncodingException e ) {
//                e.printStackTrace();
            }
        }
        if( withKeypair != null ) {
            params.add(new Param("keypair", withKeypair));
        }
        if( securityGroupIds != null && securityGroupIds.length() > 0 ) {
            params.add(new Param("securitygroupids", securityGroupIds));
        }
        if( vlans != null && vlans.size() > 0 ) {
            CloudException lastError = null;

            for( String withVlanId : vlans ) {
                params.add(new Param("networkIds", withVlanId));

                try {
                    return launch(new CSMethod(getProvider()).get(
                            DEPLOY_VIRTUAL_MACHINE,
                            params.toArray(new Param[params.size()]))
                    );
                }
                catch( CloudException e ) {
                    if( e.getMessage().contains("sufficient address capacity") ) {
                        lastError = e;
                        continue;
                    }
                    throw e;
                }
            }
            if( lastError == null ) {
                throw lastError;
            }
            throw new CloudException("Unable to identify a network into which a VM can be launched");
        }
        else {
            return launch(new CSMethod(getProvider()).get(
                    DEPLOY_VIRTUAL_MACHINE,
                    params.toArray(new Param[params.size()])
            ));
        }
    }
    
    private @Nonnull VirtualMachine launch(@Nonnull Document doc) throws InternalException, CloudException {
        NodeList matches = doc.getElementsByTagName("deployvirtualmachineresponse");
        String serverId = null;
        String jobId = null;
        
        for( int i=0; i<matches.getLength(); i++ ) {
            NodeList attrs = matches.item(i).getChildNodes();
            
            for( int j=0; j<attrs.getLength(); j++ ) {
                Node node = attrs.item(j);
                
                if( node != null && (node.getNodeName().equalsIgnoreCase("virtualmachineid") || node.getNodeName().equalsIgnoreCase("id")) ) {
                    serverId = node.getFirstChild().getNodeValue();
                    break;
                }
                else if (node != null && node.getNodeName().equalsIgnoreCase("jobid") ) {
                    jobId = node.getFirstChild().getNodeValue();
                }
            }
            if( serverId != null ) {
                break;
            }
        }
        if( serverId == null && jobId == null ) {
            throw new CloudException("Could not launch server");
        }
        // TODO: very odd logic below; figure out what it thinks it is doing
        
        VirtualMachine vm = null;

        // have to wait on jobs as sometimes they fail and we need to bubble error message up
        Document responseDoc = getProvider().waitForJob(doc, "Launch Server");

        //parse vm from job completion response to capture vm passwords on initial launch.
        if (responseDoc != null){
            NodeList nodeList = responseDoc.getElementsByTagName("virtualmachine");
            if (nodeList.getLength() > 0) {
                Node virtualMachine = nodeList.item(0);
                vm = toVirtualMachine(virtualMachine);
                if( vm != null ) {
                    return vm;
                }
            }
        }
        
        if (vm == null){
        	vm = getVirtualMachine(serverId);
        }
        if( vm == null ) {
            throw new CloudException("No virtual machine provided: " + serverId);
        }
        return vm;
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listFirewalls");
        try {
            SecurityGroup support = getProvider().getNetworkServices().getFirewallSupport();

            if( support == null ) {
                return Collections.emptyList();
            }
            return support.listFirewallsForVM(vmId);
        }
        finally {
            APITrace.end();
        }
    }

    private void setFirewalls(@Nonnull VirtualMachine vm) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.setFirewalls");
        try {
            SecurityGroup support = getProvider().getNetworkServices().getFirewallSupport();

            if( support == null ) {
                return;
            }
            List<String> ids = new ArrayList<String>();

            for( String id : support.listFirewallsForVM(vm.getProviderVirtualMachineId()) ) {
                ids.add(id);
            }
            vm.setProviderFirewallIds(ids.toArray(new String[ids.size()]));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listAllProducts() throws InternalException, CloudException{
        return listProducts(VirtualMachineProductFilterOptions.getInstance(), Architecture.I64);
    }

    @Nonnull
    @Override
    public Iterable<VirtualMachineProduct> listProducts(@Nonnull String machineImageId, @Nonnull VirtualMachineProductFilterOptions options) throws InternalException, CloudException {
        // all CS products are dual architecture so it doesn't matter which arch to choose
        return listProducts(options, Architecture.I64);
    }

    public Iterable<VirtualMachineProduct> listProducts(VirtualMachineProductFilterOptions options, Architecture architecture) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listProducts");
        try {
            Cache<VirtualMachineProduct> cache = Cache.getInstance(getProvider(), "ServerProducts", VirtualMachineProduct.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Hour>(4, TimePeriod.HOUR));
            Collection<VirtualMachineProduct> products = (Collection<VirtualMachineProduct>)cache.get(getContext());
            if(products == null){
                Set<String> mapping = null;

                if( customServiceMappings == null ) {
                    load();
                }
                if( customServiceMappings != null ) {
                    String cloudId = cloudMappings.getProperty(getContext().getCloud().getEndpoint());

                    if( cloudId != null ) {
                        Map<String,Set<String>> map = customServiceMappings.get(cloudId);

                        if( map != null ) {
                            mapping = map.get(getContext().getRegionId());
                        }
                    }
                }
                products = new ArrayList<VirtualMachineProduct>();

                Document doc = new CSMethod(getProvider()).get(
                        LIST_SERVICE_OFFERINGS,
                        new Param("zoneId", getContext().getRegionId())
                );
                NodeList matches = doc.getElementsByTagName("serviceoffering");

                for( int i=0; i<matches.getLength(); i++ ) {
                    String id = null, name = null;
                    Node node = matches.item(i);
                    NodeList attributes;
                    int memory = 0;
                    int cpu = 0;
                    Boolean customized = null;
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
                            id = value;
                        }
                        else if( n.getNodeName().equals("name") ) {
                            name = value;
                        }
                        else if( n.getNodeName().equals("cpunumber") ) {
                            cpu = Integer.parseInt(value);
                        }
                        else if( n.getNodeName().equals("memory") ) {
                            memory = Integer.parseInt(value);
                        }
                        else if( n.getNodeName().equals("iscustomized") ) {
                            customized = Boolean.valueOf(value);
                        }
                        if( id != null && name != null && cpu > 0 && memory > 0 && customized != null) {
                            break;
                        }
                    }
                    if( id != null  && name != null && cpu > 0 && memory > 0 && !customized) {
                        if( mapping == null || mapping.contains(id) ) {
                            VirtualMachineProduct product;

                            product = new VirtualMachineProduct();
                            product.setProviderProductId(id);
                            product.setName(name + " (" + cpu + " CPU/" + memory + "MB RAM)");
                            product.setDescription(name + " (" + cpu + " CPU/" + memory + "MB RAM)");
                            product.setRamSize(new Storage<Megabyte>(memory, Storage.MEGABYTE));
                            product.setCpuCount(cpu);
                            product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                            product.setArchitectures(Architecture.I32, Architecture.I64);
                            if (options != null) {
                                if (options.matches(product)) {
                                    products.add(product);
                                }
                            }
                            else {
                                products.add(product);
                            }
                        }
                    }
                }
                cache.put(getContext(), products);
            }
            return products;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listVirtualMachineStatus");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(
                    LIST_VIRTUAL_MACHINES,
                    new Param("zoneId", getContext().getRegionId())
            );
            List<ResourceStatus> servers = new ArrayList<ResourceStatus>();

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
                    doc = method.get(
                            LIST_VIRTUAL_MACHINES,
                            new Param("zoneId", getContext().getRegionId()),
                            new Param("pagesize", "500"),
                            new Param("page", nextPage)
                    );
                }
                NodeList matches = doc.getElementsByTagName("virtualmachine");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        ResourceStatus vm = toStatus(node);

                        if( vm != null ) {
                            servers.add(vm);
                        }
                    }
                }
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.listVirtualMachines");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(
                    LIST_VIRTUAL_MACHINES,
                    new Param("zoneId", getContext().getRegionId())
            );
            List<VirtualMachine> servers = new ArrayList<VirtualMachine>();

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
                    doc = method.get(
                            LIST_VIRTUAL_MACHINES,
                            new Param("zoneId", getContext().getRegionId()),
                            new Param("pagesize", "500"),
                            new Param("page", nextPage));
                }
                NodeList matches = doc.getElementsByTagName("virtualmachine");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        VirtualMachine vm = toVirtualMachine(node);

                        if( vm != null ) {
                            servers.add(vm);
                        }
                    }
                }
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    private String resetPassword(@Nonnull String serverId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.resetPassword");
        try {
            Document doc = new CSMethod(getProvider()).get(
                    RESET_VIRTUAL_MACHINE_PASSWORD,
                    new Param("id", serverId)
            );

            Document responseDoc = getProvider().waitForJob(doc, "reset vm password");

            if (responseDoc != null){
                NodeList matches = responseDoc.getElementsByTagName("virtualmachine");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        NodeList attributes = node.getChildNodes();
                        for( int j=0; j<attributes.getLength(); j++ ) {
                            Node attribute = attributes.item(j);
                            String name = attribute.getNodeName().toLowerCase();
                            String value;

                            if( attribute.getChildNodes().getLength() > 0 ) {
                                value = attribute.getFirstChild().getNodeValue();
                            }
                            else {
                                value = null;
                            }
                            if( name.equals("password") ) {
                                return value;
                            }
                        }
                    }
                }
            }


            logger.warn("Unable to find password for vm with id "+serverId);
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void reboot(@Nonnull String serverId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.reboot");
        try {
            new CSMethod(getProvider()).get(
                    REBOOT_VIRTUAL_MACHINE,
                    new Param("id", serverId)
            );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void start(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.start");
        try {
            new CSMethod(getProvider()).get(
                    START_VIRTUAL_MACHINE,
                    new Param("id", serverId)
            );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.stop");
        try {
            new CSMethod(getProvider()).get(
                    STOP_VIRTUAL_MACHINE,
                    new Param("id", vmId),
                    new Param("forced", String.valueOf(force))
            );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void terminate(@Nonnull String serverId, @Nullable String explanation) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.terminate");
        try {
            List<Param> params = new ArrayList<Param>();
            params.add(new Param("id", serverId));
            if( getProvider().isAdminAccount() ) {
                params.add(new Param("expunge", "true"));
            }
            new CSMethod(getProvider()).get(
                    DESTROY_VIRTUAL_MACHINE, params
            );
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        VmState state = null;
        String serverId = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName().toLowerCase();
            String value;

            if( attribute.getChildNodes().getLength() > 0 ) {
                value = attribute.getFirstChild().getNodeValue();
            }
            else {
                value = null;
            }
            if( name.equals("virtualmachineid") || name.equals("id") ) {
                serverId = value;
            }
            else if( name.equals("state") ) {
                if( value == null || value.equalsIgnoreCase("starting") || value.equalsIgnoreCase("creating") ) {
                    state = VmState.PENDING;
                }
                else  if( value.equalsIgnoreCase("stopped") ) {
                    state = VmState.STOPPED;
                }
                else if( value.equalsIgnoreCase("running") ) {
                    state = VmState.RUNNING;
                }
                else if( value.equalsIgnoreCase("stopping") ) {
                    state = VmState.STOPPING;
                }
                else if( value.equalsIgnoreCase("migrating") || value.equalsIgnoreCase("ha") ) {
                    state = VmState.REBOOTING;
                }
                else if( value.equalsIgnoreCase("destroyed") || value.equalsIgnoreCase("expunging") ) {
                    state = VmState.TERMINATED;
                }
                else if( value.equalsIgnoreCase("error") ) {
                    state = VmState.ERROR;
                }
                else {
                    throw new CloudException("Unexpected server state: " + value);
                }
            }
            if( serverId != null && state != null ) {
                break;
            }
        }
        if( serverId == null ) {
            return null;
        }
        if( state == null ) {
            state = VmState.PENDING;
        }
        return new ResourceStatus(serverId, state);
    }

    private @Nullable VirtualMachine toVirtualMachine(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        HashMap<String,String> properties = new HashMap<String,String>();
        VirtualMachine server = new VirtualMachine();
        NodeList attributes = node.getChildNodes();
        String productId = null;
        
        server.setProviderOwnerId(getContext().getAccountNumber());
        server.setClonable(false);
        server.setImagable(false);
        server.setPausable(true);
        server.setPersistent(true);
        server.setArchitecture(Architecture.I64);
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName().toLowerCase();
            String value;
            
            if( attribute.getChildNodes().getLength() > 0 ) {
                value = attribute.getFirstChild().getNodeValue();                
            }
            else {
                value = null;
            }
            if( name.equals("virtualmachineid") || name.equals("id") ) {
                server.setProviderVirtualMachineId(value);                
            }
            else if( name.equals("name") ) {
                server.setDescription(value);
            }
            /*
            else if( name.equals("haenable") ) {
                server.setPersistent(value != null && value.equalsIgnoreCase("true"));
            }
            */
            else if( name.equals("displayname") ) {
                server.setName(value);
            }
            else if( name.equals("ipaddress") ) { // v2.1
                if( value != null ) {
                    server.setPrivateAddresses(new RawAddress(value));
                }
                server.setPrivateDnsAddress(value);
            }
            else if( name.equals("password") ) {
                server.setRootPassword(value);
            }
            else if( name.equals("securitygroup") ) { // v2.2+
                if( attribute.hasChildNodes() ) {
                    NodeList parts = attribute.getChildNodes();
                    String sgId = null, sgName = null, sgDescription = null;
                    for( int j=0; j<parts.getLength(); j++ ) {
                        Node part = parts.item(j);
                        if( "id".equalsIgnoreCase(part.getNodeName()) ) {
                            server.setProviderFirewallIds(new String[] { part.getFirstChild().getNodeValue() });
                            break;
                        }
                    }
                }
            }
            else if( name.equals("nic") ) { // v2.2+
                if( attribute.hasChildNodes() ) {                    
                    NodeList parts = attribute.getChildNodes();
                    String addr = null;
                    
                    for( int j=0; j<parts.getLength(); j++ ) {
                        Node part = parts.item(j); 
                        
                        if( part.getNodeName().equalsIgnoreCase("ipaddress") ) {
                            if( part.hasChildNodes() ) {
                                addr = part.getFirstChild().getNodeValue();
                                if( addr != null ) {
                                    addr = addr.trim();
                                }
                            }
                        }
                        else if( part.getNodeName().equalsIgnoreCase("networkid") ) {
                            server.setProviderVlanId(part.getFirstChild().getNodeValue().trim());
                        }
                    }
                    if( addr != null ) {
                        boolean pub = false;
                        
                        if( !addr.startsWith("10.") && !addr.startsWith("192.168.") ) {
                            if( addr.startsWith("172.") ) {
                                String[] nums = addr.split("\\.");
                                
                                if( nums.length != 4 ) {
                                    pub = true;
                                }
                                else {
                                    try {
                                        int x = Integer.parseInt(nums[1]);
                                        
                                        if( x < 16 || x > 31 ) {
                                            pub = true;
                                        }
                                    }
                                    catch( NumberFormatException ignore ) {
                                        // ignore
                                    }
                                }
                            }
                            else {
                                pub = true;
                            }
                        }
                        if( pub ) {
                            server.setPublicAddresses(new RawAddress(addr));
                            if( server.getPublicDnsAddress() == null ) {
                                server.setPublicDnsAddress(addr);
                            }
                        }
                        else {
                            server.setPrivateAddresses(new RawAddress(addr));
                            if( server.getPrivateDnsAddress() == null ) {
                                server.setPrivateDnsAddress(addr);
                            }
                        }
                    }
                }
            }
            else if( name.equals("osarchitecture") ) {
                if( value != null && value.equals("32") ) {
                    server.setArchitecture(Architecture.I32);
                }
                else {
                    server.setArchitecture(Architecture.I64);                  
                }
            }
            else if( name.equals("created") ) {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); //2009-02-03T05:26:32.612278
                
                try {
                    server.setCreationTimestamp(df.parse(value).getTime());
                }
                catch( ParseException e ) {
                    logger.warn("Invalid date: " + value);
                    server.setLastBootTimestamp(0L);
                }
            }
            else if( name.equals("state") ) {
                VmState state;

                //(Running, Stopped, Stopping, Starting, Creating, Migrating, HA).
                if( value.equalsIgnoreCase("stopped") ) {
                    state = VmState.STOPPED;
                    server.setImagable(true);
                }
                else if( value.equalsIgnoreCase("running") ) {
                    state = VmState.RUNNING;
                }
                else if( value.equalsIgnoreCase("stopping") ) {
                    state = VmState.STOPPING;
                }
                else if( value.equalsIgnoreCase("starting") ) {
                    state = VmState.PENDING;
                }
                else if( value.equalsIgnoreCase("creating") ) {
                    state = VmState.PENDING;
                }
                else if( value.equalsIgnoreCase("migrating") ) {
                    state = VmState.REBOOTING;
                }
                else if( value.equalsIgnoreCase("destroyed") ) {
                    state = VmState.TERMINATED;
                }
                else if( value.equalsIgnoreCase("error") ) {
                    state = VmState.ERROR;
                }
                else if( value.equalsIgnoreCase("expunging") ) {
                    state = VmState.TERMINATED;
                }
                else if( value.equalsIgnoreCase("ha") ) {
                    state = VmState.REBOOTING;
                }
                else {
                    throw new CloudException("Unexpected server state: " + value);
                }
                server.setCurrentState(state);                
            }
            else if( name.equals("zoneid") ) {
                server.setProviderRegionId(value);
                server.setProviderDataCenterId(value);
            }
            else if( name.equals("templateid") ) {
                server.setProviderMachineImageId(value);
            }
            else if( name.equals("templatename") ) {
                server.setPlatform(Platform.guess(value));
            }
            else if( name.equals("serviceofferingid") ) {
                productId = value;
            }
            else if( name.equals("keypair") ) {
                server.setProviderKeypairId(value);
            }
            else if( value != null ) {
                properties.put(name, value);
            }
        }
        if( server.getName() == null ) {
            server.setName(server.getProviderVirtualMachineId());
        }
        if( server.getDescription() == null ) {
            server.setDescription(server.getName());
        }
        server.setProviderAssignedIpAddressId(null);
        if( server.getProviderRegionId() == null ) {
            server.setProviderRegionId(getContext().getRegionId());
        }
        if( server.getProviderDataCenterId() == null ) {
            server.setProviderDataCenterId(getContext().getRegionId());
        }
        if( productId != null ) {
            server.setProductId(productId);
        }

        /*final String finalServerId = server.getProviderVirtualMachineId();
        // commenting out for now until we can find a way to return plain text rather than encrypted
            server.setPasswordCallback(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return getRootPassword(finalServerId);
            }
        }
        );  */
        server.setTags(properties);
        return server;
    }
    
    @Override
    public void setTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	setTags(new String[] { vmId }, tags);
    }
    
    @Override
    public void setTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.setTags");
    	try {
    		removeTags(vmIds);
    		getProvider().createTags(vmIds, "UserVm", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	updateTags(new String[] { vmId }, tags);
    }
    
    @Override
    public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.updateTags");
    	try {
    		getProvider().updateTags(vmIds, "UserVm", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void removeTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	removeTags(new String[] { vmId }, tags);
    }
    
    @Override
    public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "Server.removeTags");
    	try {
    		getProvider().removeTags(vmIds, "UserVm", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
}
