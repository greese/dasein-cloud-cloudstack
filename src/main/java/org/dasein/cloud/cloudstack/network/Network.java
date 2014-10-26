/**
 * Copyright (C) 2009-2014 Dell, Inc.
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.network.AbstractVLANSupport;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.InternetGateway;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.Networkable;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.RoutingTable;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANCapabilities;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Hour;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Network extends AbstractVLANSupport<CSCloud> {
    static public final Logger logger = Logger.getLogger(Network.class);

    static public final String CREATE_NETWORK         = "createNetwork";
    static public final String DELETE_NETWORK         = "deleteNetwork";
    static public final String LIST_NETWORK_OFFERINGS = "listNetworkOfferings";
    static public final String LIST_NETWORKS          = "listNetworks";

    static public final String CREATE_EGRESS_RULE     = "createEgressFirewallRule";

    Network(CSCloud provider) {
        super(provider);
    }

    public List<String> findFreeNetworks() throws CloudException, InternalException {
        ArrayList<String> vlans = new ArrayList<String>();

        for( VLAN n : listDefaultNetworks(true, true) ) {
            if( n != null ) {
                vlans.add(n.getProviderVlanId());
            }
        }
        for( VLAN n : listDefaultNetworks(false, true) ) {
            if( n != null && !vlans.contains(n.getProviderVlanId()) ) {
                vlans.add(n.getProviderVlanId());
            }
        }
        return vlans;
    }

    private transient volatile CSVlanCapabilities capabilities;
    @Nonnull
    @Override
    public VLANCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new CSVlanCapabilities(getProvider());
        }
        return capabilities;
    }

    static public class NetworkOffering {
        public String availability;
        public String networkType;
        public String offeringId;
    }
    
    public @Nonnull Collection<NetworkOffering> getNetworkOfferings(@Nonnull String regionId) throws InternalException, CloudException {
        Cache<NetworkOffering> cache = null;

        if( regionId.equals(getContext().getRegionId()) ) {
            cache = Cache.getInstance(getProvider(), "networkOfferings", NetworkOffering.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Hour>(1, TimePeriod.HOUR));

            Collection<NetworkOffering> offerings = (Collection<NetworkOffering>)cache.get(getContext());

            if( offerings != null ) {
                return offerings;
            }
        }
        CSMethod method = new CSMethod(getProvider());
        Document doc = method.get(method.buildUrl(LIST_NETWORK_OFFERINGS, new Param("zoneId", regionId)), LIST_NETWORK_OFFERINGS);
        NodeList matches = doc.getElementsByTagName("networkoffering");
        ArrayList<NetworkOffering> offerings = new ArrayList<NetworkOffering>();
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            NodeList attributes = node.getChildNodes();
            NetworkOffering offering = new NetworkOffering();

            for( int j=0; j<attributes.getLength(); j++ ) {
                Node n = attributes.item(j);
                String value;

                if( n.getChildNodes().getLength() > 0 ) {
                    value = n.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( n.getNodeName().equals("id") && value != null ) {
                    offering.offeringId = value.trim();
                }
                else if( n.getNodeName().equalsIgnoreCase("availability") ) {
                    offering.availability = (value == null ? "unavailable" : value.trim());
                }
                else if( n.getNodeName().equalsIgnoreCase("guestiptype") ) {
                    offering.networkType = (value == null ? "direct" : value.trim());
                }
            }
            offerings.add(offering);
        }
        if( cache != null ) {
            cache.put(getContext(), offerings);
        }
        return offerings;
    }
    
    private @Nullable String getNetworkOffering(@Nonnull String regionId) throws InternalException, CloudException {
        for( NetworkOffering offering : getNetworkOfferings(regionId) ) {
            if( !offering.availability.equalsIgnoreCase("unavailable") && offering.networkType.equals("Isolated") ) {
                return offering.offeringId;
            }
        }
        return null;
    }
    
    @Override
    public @Nullable VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.getVlan");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            try {
                CSMethod method = new CSMethod(getProvider());
                Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", ctx.getRegionId()), new Param("id", vlanId)), Network.LIST_NETWORKS);
                NodeList matches = doc.getElementsByTagName("network");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        VLAN vlan = toNetwork(node, ctx);

                        if( vlan != null ) {
                            return vlan;
                        }
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
        }
        finally {
            APITrace.end();
        }
    }

    @Nullable
    @Override
    public String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nullable
    @Override
    public InternetGateway getInternetGatewayById(@Nonnull String gatewayId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.isSubscribed");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was established");
            }
            CSMethod method = new CSMethod(getProvider());

            try {
                method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", ctx.getRegionId())), Network.LIST_NETWORKS);
                return true;
            }
            catch( CSException e ) {
                int code = e.getHttpCode();

                if( code == HttpServletResponse.SC_FORBIDDEN || code == 401 || code == 531 ) {
                    return false;
                }
                throw e;
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlans");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was established");
            }
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", ctx.getRegionId()), new Param("canusefordeploy", "true")), Network.LIST_NETWORKS);
            ArrayList<VLAN> networks = new ArrayList<VLAN>();

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
                    doc = method.get(method.buildUrl(LIST_NETWORKS, new Param("zoneId", ctx.getRegionId()), new Param("pagesize", "500"), new Param("page", nextPage), new Param("canusefordeploy", "true")), LIST_NETWORKS);
                }
                NodeList matches = doc.getElementsByTagName("network");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        VLAN vlan = toNetwork(node, ctx);

                        if( vlan != null ) {
                            networks.add(vlan);
                        }
                    }
                }
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeInternetGatewayById(@Nonnull String id) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public @Nonnull Iterable<VLAN> listDefaultNetworks(boolean shared, boolean forDeploy) throws CloudException, InternalException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new InternalException("No context was set for this request");
        }
        CSMethod method = new CSMethod(getProvider());
        Param[] params;

        if( !shared && forDeploy ) {
            params = new Param[3];
        }
        else if( !shared || forDeploy ) {
            params = new Param[2];
        }
        else {
            params = new Param[1];
        }
        params[0] = new Param("zoneId", ctx.getRegionId());
        int idx = 1;
        if( forDeploy ) {
            params[idx++]  = new Param("canUseForDeploy", "true");
        }
        if( !shared ) {
            params[idx] = new Param("account", ctx.getAccountNumber());
        }
        Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, params), Network.LIST_NETWORKS);
        ArrayList<VLAN> networks = new ArrayList<VLAN>();
        NodeList matches = doc.getElementsByTagName("network");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
                
            if( node != null ) {
                VLAN vlan = toNetwork(node, ctx);
                    
                if( vlan != null ) {
                    if (vlan.getTag("displaynetwork") == null || vlan.getTag("displaynetwork").equals("true")) {
                        if (vlan.getTag("isdefault") == null || vlan.getTag("isdefault").equals("true")) {
                            networks.add(vlan);
                        }
                    }
                }
            }
        }
        return networks;        
    }
    
    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nullable String domainName, @Nullable String[] dnsServers, @Nullable String[] ntpServers) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.createVlan");
        try {
            if( !getCapabilities().allowsNewVlanCreation() ) {
                throw new OperationNotSupportedException();
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }
            String offering = getNetworkOffering(regionId);

            if( offering == null ) {
                throw new CloudException("No offerings exist for " + ctx.getRegionId());
            }

            String[] parts = cidr.split("/");
            String gateway = "", netmask = "";
            if (parts.length == 1) {
                gateway = parts[0];
                netmask = "255.255.255.255";
            }

            if (parts.length == 2) {
                gateway = parts[0];
                netmask = parts[1];
                int prefix = Integer.parseInt(netmask);
                int mask = 0xffffffff << (32 - prefix);

                int value = mask;
                byte[] bytes = new byte[]{
                        (byte)(value >>> 24), (byte)(value >> 16 & 0xff), (byte)(value >> 8 & 0xff), (byte)(value & 0xff) };

                try {
                    InetAddress netAddr = InetAddress.getByAddress(bytes);
                    netmask = netAddr.getHostAddress();
                }
                catch (UnknownHostException e) {
                    throw new InternalException("Unable to parse netmask from "+cidr);
                }
            }

            CSMethod method = new CSMethod(getProvider());
            Document doc;
            if (parts.length > 0) {
                doc = method.get(method.buildUrl(CREATE_NETWORK, new Param("zoneId", ctx.getRegionId()), new Param("networkOfferingId", offering), new Param("name", name), new Param("displayText", name), new Param("gateway", gateway), new Param("netmask", netmask)), CREATE_NETWORK);
            }
            else {
                doc = method.get(method.buildUrl(CREATE_NETWORK, new Param("zoneId", ctx.getRegionId()), new Param("networkOfferingId", offering), new Param("name", name), new Param("displayText", name)), CREATE_NETWORK);
            }
            NodeList matches = doc.getElementsByTagName("network");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);

                if( node != null ) {
                    VLAN network = toNetwork(node, ctx);

                    if( network != null ) {
                        // create default egress rule
                        try {
                            method.get(method.buildUrl(CREATE_EGRESS_RULE, new Param("protocol", "All"), new Param("cidrlist", "0.0.0.0/0"), new Param("networkid", network.getProviderVlanId())), CREATE_EGRESS_RULE);
                        }
                        catch (Throwable ignore) {
                            logger.warn("Unable to create default egress rule");
                        }
                        return network;
                    }
                }
            }
            throw new CloudException("Creation requested failed to create a network without an error");
        }
        finally {
            APITrace.end();
        }
    }

    public @Nullable VLAN toNetwork(@Nullable Node node, @Nonnull ProviderContext ctx) {
        if( node == null ) {
            return null;
        }
        String netmask = null;
        VLAN network = new VLAN();
        String gateway = null;

        NodeList attributes = node.getChildNodes();

        network.setProviderOwnerId(ctx.getAccountNumber());
        network.setProviderRegionId(ctx.getRegionId());
        network.setCurrentState(VLANState.AVAILABLE);
        network.setSupportedTraffic(new IPVersion[] { IPVersion.IPV4 });
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
            if( name.equalsIgnoreCase("id") ) {
                network.setProviderVlanId(value);
            }
            else if( name.equalsIgnoreCase("name") ) {
                if( network.getName() == null ) {
                    network.setName(value);
                }
            }
            else if( name.equalsIgnoreCase("displaytext") ) {
                network.setName(value);
            }
            else if( name.equalsIgnoreCase("displaynetwork")) {
                network.setTag("displaynetwork", value);
            }
            else if( name.equalsIgnoreCase("isdefault")) {
                network.setTag("isdefault", value);
            }
            else if( name.equalsIgnoreCase("networkdomain") ) {
                network.setDomainName(value);
            }
            else if( name.equalsIgnoreCase("zoneid") && value != null ) {
                network.setProviderRegionId(value);
            }
            else if( name.startsWith("dns") && value != null && !value.trim().equals("") ) {
                String[] dns;
                
                if( network.getDnsServers() != null ) {
                    dns = new String[network.getDnsServers().length+1];
                    for( int idx=0; idx<network.getDnsServers().length; idx++ ) {
                        dns[idx] = network.getDnsServers()[idx];
                    }
                    dns[dns.length-1] = value;
                }
                else {
                    dns = new String[] { value };
                }
                network.setDnsServers(dns);
            }
            else if( name.equalsIgnoreCase("netmask") ) {
                netmask = value;
            }
            else if( name.equals("gateway") ) {
                gateway = value;
            }
            else if( name.equalsIgnoreCase("networkofferingdisplaytext") ) {
                network.setNetworkType(value);
            }
            else if( name.equalsIgnoreCase("account") ) {
                network.setProviderOwnerId(value);
            }
        }
        if( network.getProviderVlanId() == null ) {
            return null;
        }
        network.setProviderDataCenterId(network.getProviderRegionId());
        if( network.getName() == null ) {
            network.setName(network.getProviderVlanId());
        }
        if( network.getDescription() == null ) {
            network.setDescription(network.getName());
        }
        if( gateway != null ) {
            if( netmask == null ) {
                netmask = "255.255.255.0";
            }
            network.setCidr(netmask, gateway);
        }
        return network;
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "NIC";
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "network";
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return "network";
    }

    @Nonnull
    @Override
    public Collection<InternetGateway> listInternetGateways(@Nullable String vlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Networkable> listResources(@Nonnull String inVlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listResources");
        try {
            ArrayList<Networkable> resources = new ArrayList<Networkable>();
            NetworkServices network = getProvider().getNetworkServices();

            FirewallSupport fwSupport = network.getFirewallSupport();

            if( fwSupport != null ) {
                for( Firewall fw : fwSupport.list() ) {
                    if( inVlanId.equals(fw.getProviderVlanId()) ) {
                        resources.add(fw);
                    }
                }
            }

            IpAddressSupport ipSupport = network.getIpAddressSupport();

            if( ipSupport != null ) {
                for( IPVersion version : ipSupport.getCapabilities().listSupportedIPVersions() ) {
                    for( org.dasein.cloud.network.IpAddress addr : ipSupport.listIpPool(version, false) ) {
                        if( inVlanId.equals(addr.getProviderVlanId()) ) {
                            resources.add(addr);
                        }
                    }

                }
            }
            for( RoutingTable table : listRoutingTables(inVlanId) ) {
                resources.add(table);
            }
            Iterable<VirtualMachine> vms = getProvider().getComputeServices().getVirtualMachineSupport().listVirtualMachines();

            for( VirtualMachine vm : vms ) {
                if( inVlanId.equals(vm.getProviderVlanId()) ) {
                    resources.add(vm);
                }
            }
            return resources;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlanStatus");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was established");
            }
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", ctx.getRegionId()), new Param("canusefordeploy", "true")), Network.LIST_NETWORKS);
            ArrayList<ResourceStatus> networks = new ArrayList<ResourceStatus>();

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
                    doc = method.get(method.buildUrl(LIST_NETWORKS, new Param("zoneId", ctx.getRegionId()), new Param("pagesize", "500"), new Param("page", nextPage), new Param("canusefordeploy", "true")), LIST_NETWORKS);
                }
                NodeList matches = doc.getElementsByTagName("network");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node node = matches.item(i);

                    if( node != null ) {
                        ResourceStatus vlan = toVLANStatus(node);

                        if( vlan != null ) {
                            networks.add(vlan);
                        }
                    }
                }
            }
            return networks;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removeVlan");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(DELETE_NETWORK, new Param("id", vlanId)), DELETE_NETWORK);

            getProvider().waitForJob(doc, "Delete VLAN");
        }
        finally {
            APITrace.end();
        }
    }

    public @Nullable ResourceStatus toVLANStatus(@Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        String networkId = null;

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
            if( name.equalsIgnoreCase("id") ) {
                networkId = value;
                break;
            }
        }
        if( networkId == null ) {
            return null;
        }
        return new ResourceStatus(networkId, VLANState.AVAILABLE);
    }

}
