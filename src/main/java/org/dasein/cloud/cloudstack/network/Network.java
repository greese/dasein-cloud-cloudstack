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

package org.dasein.cloud.cloudstack.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.cloudstack.CloudstackException;
import org.dasein.cloud.cloudstack.CloudstackMethod;
import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.cloudstack.Zones;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.NetworkInterface;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.network.VLAN;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Network implements VLANSupport {
    static public final String CREATE_NETWORK         = "createNetwork";
    static public final String LIST_NETWORK_OFFERINGS = "listNetworkOfferings";
    static public final String LIST_NETWORKS          = "listNetworks";
    
    private CloudstackProvider cloudstack;
    
    Network(CloudstackProvider cloudstack) { this.cloudstack = cloudstack; }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return false;
    }
    
    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(Zones.LIST_ZONES, new Param[] { new Param("id", cloudstack.getContext().getRegionId()) }));
        NodeList matches = doc.getElementsByTagName("zone");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            NodeList attrs = node.getChildNodes();
            
            for( int j=0; j<attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);
                                                         
                if( attr.getNodeName().equalsIgnoreCase("securitygroupsenabled") ) {
                    String val = null;
                    
                    if( attr.hasChildNodes() ) {
                        val = attr.getFirstChild().getNodeValue().trim();
                    }
                    if( val != null && val.equalsIgnoreCase("true") ) {
                        return true;
                    }
                }
            }
        }
        return false;
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

    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return 1;
    }

    static public class NetworkOffering {
        public String availability;
        public String networkType;
        public String offeringId;
    }
    
    public Collection<NetworkOffering> getNetworkOfferings(String inZoneId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_NETWORK_OFFERINGS, new Param[] { new Param("zoneId", inZoneId) }));
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
                if( n.getNodeName().equals("id") ) {
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
        return offerings;
    }
    
    private String getNetworkOffering(String inZoneId) throws InternalException, CloudException {
        for( NetworkOffering offering : getNetworkOfferings(inZoneId) ) {
            if( !offering.availability.equalsIgnoreCase("unavailable") && offering.networkType.equals("virtual") ) {
                return offering.offeringId;
            }
        }
        return null;
    }
    
    @Override
    public VLAN getVlan(String vlanId) throws CloudException, InternalException {
        try {
            CloudstackMethod method = new CloudstackMethod(cloudstack);
            Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", cloudstack.getContext().getRegionId()), new Param("id", vlanId)));
            NodeList matches = doc.getElementsByTagName("network");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);

                if( node != null ) {
                    VLAN vlan = toNetwork(node);

                    if( vlan != null ) {
                        return vlan;
                    }
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

    public boolean isBasicNetworking() throws CloudException, InternalException {
        ProviderContext ctx = cloudstack.getContext();
        
        if( ctx == null ) {
            throw new InternalException("No context was established");
        }
        String regionId = ctx.getRegionId();

        CloudstackMethod method = new CloudstackMethod(cloudstack);
        String url = method.buildUrl(Zones.LIST_ZONES, new Param("available", "true"));
        Document doc = method.get(url);

        NodeList matches = doc.getElementsByTagName("zone");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node zone = matches.item(i);
            NodeList attrs = zone.getChildNodes();
            String id = null, networking = "basic";

            for( int j=0; j<attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);
                String nn = attr.getNodeName();

                if( nn.equalsIgnoreCase("networktype") && attr.hasChildNodes() ) {
                    networking = attr.getFirstChild().getNodeValue().trim();
                }
                else if ( nn.equalsIgnoreCase("id") && attr.hasChildNodes() ) {
                    id = attr.getFirstChild().getNodeValue().trim();
                }
            }
            if( id != null && id.equals(regionId) ) {
                return networking.equalsIgnoreCase("basic");
            }
        }
        return true;
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        
        try {
            method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", cloudstack.getContext().getRegionId())));
            return true;
        }
        catch( CloudstackException e ) {
            int code = e.getHttpCode();

            if( code == HttpServletResponse.SC_FORBIDDEN || code == 401 || code == 531 ) {
                return false;
            }
            throw e;
        }
    }
    
    @Override
    public Iterable<VLAN> listVlans() throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, new Param("zoneId", cloudstack.getContext().getRegionId())));
        ArrayList<VLAN> networks = new ArrayList<VLAN>();
        NodeList matches = doc.getElementsByTagName("network");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
                
            if( node != null ) {
                VLAN vlan = toNetwork(node);
                    
                if( vlan != null ) {
                    networks.add(vlan);
                }
            }
        }
        return networks;
    }

    public Iterable<VLAN> listDefaultNetworks(boolean shared, boolean forDeploy) throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Param[] params;

        if( !shared && forDeploy ) {
            params = new Param[4];
        }
        else if( !shared || forDeploy ) {
            params = new Param[3];
        }
        else {
            params = new Param[2];
        }
        params[0] = new Param("zoneId", cloudstack.getContext().getRegionId());
        params[1] = new Param("isdefault", "true");
        int idx = 2;
        if( forDeploy ) {
            params[idx++]  = new Param("canUseForDeploy", "true");
        }
        if( !shared ) {
            params[idx] = new Param("account", cloudstack.getContext().getAccountNumber());
        }
        Document doc = method.get(method.buildUrl(Network.LIST_NETWORKS, params));
        ArrayList<VLAN> networks = new ArrayList<VLAN>();
        NodeList matches = doc.getElementsByTagName("network");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
                
            if( node != null ) {
                VLAN vlan = toNetwork(node);
                    
                if( vlan != null ) {
                    networks.add(vlan);
                }
            }
        }
        return networks;        
    }
    
    @Override
    public VLAN createVlan(String cidr, String name, String description, String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        if( !allowsNewVlanCreation() ) {
            throw new OperationNotSupportedException();
        }
        ProviderContext ctx = cloudstack.getContext();
        
        if( ctx == null ) {
            throw new InternalException("No context was set for this request");
        }
        String offering = getNetworkOffering(ctx.getRegionId());
        
        if( offering == null ) {
            throw new CloudException("No offerings exist for " + ctx.getRegionId());
        }
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(CREATE_NETWORK, new Param[] { new Param("zoneId", ctx.getRegionId()), new Param("networkOfferingId", offering), new Param("name", name), new Param("displayText", name) }));
        // TODO: specify subnet
        NodeList matches = doc.getElementsByTagName("network");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                VLAN network = toNetwork(node);
                
                if( network != null ) {
                    return network;
                }
            }
        }
        throw new CloudException("Creation requested failed to create a network without an error");
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException { 
        // TODO Auto-generated method stub
        
    }

    public VLAN toNetwork(Node node) {
        if( node == null ) {
            return null;
        }
        String netmask = null;
        VLAN network = new VLAN();
        
        NodeList attributes = node.getChildNodes();

        network.setProviderOwnerId(cloudstack.getContext().getAccountNumber());
        network.setProviderRegionId(cloudstack.getContext().getRegionId());
        network.setCurrentState(VLANState.AVAILABLE);
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
                network.setGateway(value);
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
        if( network.getGateway() != null ) {
            if( netmask == null ) {
                netmask = "255.255.255.0";
            }
            network.setCidr(toCidr(network.getGateway(), netmask));
        }
        return network;
    }

    @Override
    public Iterable<NetworkInterface> listNetworkInterfaces(String arg0) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public Subnet createSubnet(String cidr, String inProviderVlanId, String name, String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Unable to create subnets");
    }

    @Override
    public String getProviderTermForNetworkInterface(Locale locale) {
        return "NIC";
    }

    @Override
    public String getProviderTermForSubnet(Locale locale) {
        return "network";
    }

    @Override
    public String getProviderTermForVlan(Locale locale) {
        return "network";
    }

    @Override
    public Subnet getSubnet(String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException {
        return true;
    }
    
    @Override
    public Iterable<Subnet> listSubnets(String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        throw new OperationNotSupportedException();
    }

    @Override
    public boolean supportsVlansWithSubnets() throws CloudException, InternalException {
        return false;
    }
    
    private String toCidr(String gateway, String netmask) {
        String[] dots = netmask.split("\\.");
        int cidr = 0;
        
        for( String item : dots ) {
            int x = Integer.parseInt(item);
            
            for( ; x > 0 ; x = (x<<1)%256 ) {
                cidr++;
            }
        }
        StringBuilder network = new StringBuilder();
        
        dots = gateway.split("\\.");
        int start = 0;
        
        for( String item : dots ) {
            if( ((start+8) < cidr) || cidr == 0 ) {
                network.append(item);
            }
            else {
                int addresses = (int)Math.pow(2, (start+8)-cidr);
                int subnets = 256/addresses;
                int gw = Integer.parseInt(item);
                
                for( int i=0; i<subnets; i++ ) {
                    int base = i*addresses;
                    int top = ((i+1)*addresses);
                    
                    if( gw >= base && gw < top ) {
                        network.append(String.valueOf(base));
                        break;
                    }
                }
            }
            start += 8;
            if( start < 32 ) {
                network.append(".");
            }
        }
        network.append("/");
        network.append(String.valueOf(cidr));
        return network.toString();
    }
}
