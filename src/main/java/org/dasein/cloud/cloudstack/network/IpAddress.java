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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.cloudstack.CloudstackException;
import org.dasein.cloud.cloudstack.CloudstackMethod;
import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.cloudstack.CloudstackVersion;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.cloudstack.ServiceProvider;
import org.dasein.cloud.cloudstack.Zones;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AddressType;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.IpForwardingRule;
import org.dasein.cloud.network.LoadBalancer;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class IpAddress implements IpAddressSupport { 
	
	static private final Logger logger = Logger.getLogger(IpAddress.class);
    static private final String ASSOCIATE_IP_ADDRESS                 = "associateIpAddress";
    static private final String CREATE_PORT_FORWARDING_RULE          = "createPortForwardingRule";
    static private final String DISASSOCIATE_IP_ADDRESS              = "disassociateIpAddress";
    static private final String LIST_PORT_FORWARDING_RULES           = "listPortForwardingRules";
    static private final String LIST_PUBLIC_IP_ADDRESSES             = "listPublicIpAddresses";
    static private final String STOP_FORWARD                         = "deletePortForwardingRule";
    
    static private final String CREATE_FIREWALLRULE                 = "createFirewallRule";
    static private final String DELETE_FIREWALLRULE                 = "deleteFirewallRule";
    static private final String LIST_FIREWALLRULE                 	 = "listFirewallRules";
    
    private CloudstackProvider provider;
    
    public IpAddress(CloudstackProvider provider) {
        this.provider = provider;
    }
    
    public void assign(String addressId, String toServerId) throws InternalException, CloudException {
        throw new OperationNotSupportedException();
    }
    
    public String createFirewallRule(String ipaddressId, String cidr, Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        Param[] params = new Param[] { new Param("ipaddressid", ipaddressId), new Param("cidrlist", cidr), new Param("startport", String.valueOf(beginPort)), new Param("endport", String.valueOf(endPort)), new Param("protocol", protocol.name()) };
        CloudstackMethod method = new CloudstackMethod(provider);
        
        method.get(method.buildUrl(CREATE_FIREWALLRULE, params));
        for( FirewallRule rule : listFirewallRule(ipaddressId) ) {
            if( rule.getCidr().equals(cidr) ) {
                if( rule.getProtocol().equals(protocol) ) {
                    if( rule.getStartPort() == beginPort ) {
                        if( rule.getEndPort() == endPort ) {
                            return rule.getProviderRuleId();
                        }
                    }
                }
            }
        }
        return null;
    }
    
    public void deleteFirewallRule(String ipAddressId, String cidr, Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        FirewallRule rule = null;
        
        for( FirewallRule r : listFirewallRule(ipAddressId) ) {
            if( r.getCidr().equals(cidr) ) {
                if( r.getProtocol().equals(protocol) ) {
                    if( r.getStartPort() == beginPort ) {
                        if( r.getEndPort() == endPort ) {
                            rule = r;
                            break;
                        }
                    }
                }
            }
        }
        if( rule == null ) {
            logger.warn("No such rule for " + ipAddressId + ": " + cidr + "/" + protocol + "/" + beginPort + "/" + endPort);
            return;
        }
        Param[] params = new Param[] { new Param("id", rule.getProviderRuleId()) };
        CloudstackMethod method = new CloudstackMethod(provider);
        
        method.get(method.buildUrl(DELETE_FIREWALLRULE, params));
           
    }
    
    public Collection<FirewallRule> listFirewallRule(String ipAddressId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_FIREWALLRULE, new Param[] {  new Param("ipaddressid", ipAddressId) }));
        ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();
        
        NodeList matches = doc.getElementsByTagName("firewallrule");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                FirewallRule rule = toRule(ipAddressId, node);
                
                if( rule != null ) {
                    rules.add(rule);
                }
            }
        }
        return rules;
    }
    
    public String forward(String addressId, int publicPort, Protocol protocol, int privatePort, String onServerId) throws InternalException, CloudException {
        Logger logger = CloudstackProvider.getLogger(IpAddress.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + IpAddress.class.getName() + ".forward(" + addressId + "," + publicPort + "," + protocol + "," + privatePort + "," + onServerId + ")");
        }
        try {
            VirtualMachine server = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(onServerId);
            String privateIpAddress = null;
                
            if( server == null ) {
                throw new CloudException("No such server: " + onServerId);
            }
            for( String addr : server.getPrivateIpAddresses() ) {
                privateIpAddress = addr;
                break;
            }
            if( privateIpAddress == null ) {
                throw new CloudException("Could not determine a private IP address for " + onServerId);
            }
            Param[] params;
                
            if( isId() ) {
                params = new Param[] { new Param("ipAddressId", addressId), new Param("publicPort", String.valueOf(publicPort)), new Param("privatePort", String.valueOf(privatePort)), new Param("protocol", protocol.name()), new Param("virtualMachineId", onServerId)};
            }
            else {
                params = new Param[] { new Param("ipAddress", addressId), new Param("publicPort", String.valueOf(publicPort)), new Param("privatePort", String.valueOf(privatePort)), new Param("protocol", protocol.name()), new Param("virtualMachineId", onServerId)};
            }
            CloudstackMethod method = new CloudstackMethod(provider);
            Document doc = method.get(method.buildUrl(CREATE_PORT_FORWARDING_RULE, params));
    
            provider.waitForJob(doc, "Assigning forwarding rule");
            for( IpForwardingRule rule : listRules(addressId) ) {
                if( rule.getPublicPort() == publicPort && rule.getPrivatePort() == privatePort && onServerId.equals(rule.getServerId()) && protocol.equals(rule.getProtocol()) && addressId.equals(rule.getAddressId()) ) {
                    return rule.getProviderRuleId();
                }
            }
            return null;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + IpAddress.class.getName() + ".forward()");
            }
        }
    }
    
    private boolean isId() {
        return provider.getVersion().greaterThan(CloudstackVersion.CS21);
    }
    
    public org.dasein.cloud.network.IpAddress getIpAddress(String addressId) throws InternalException, CloudException {
        try {
            CloudstackMethod method = new CloudstackMethod(provider);
            Document doc = method.get(method.buildUrl(LIST_PUBLIC_IP_ADDRESSES, new Param[] { new Param(isId() ? "ipAddressId" : "ipAddress", addressId) }));
            HashMap<String,LoadBalancer> loadBalancers = new HashMap<String,LoadBalancer>();
            LoadBalancer lb = provider.getNetworkServices().getLoadBalancerSupport().getLoadBalancer(addressId);

            loadBalancers.put(addressId, lb);
            NodeList matches = doc.getElementsByTagName("publicipaddress");
            for( int i=0; i<matches.getLength(); i++ ) {
                org.dasein.cloud.network.IpAddress addr = toAddress(matches.item(i), loadBalancers);

                if( addr != null ) {
                    if( addr.getProviderIpAddressId().equals(addressId) ) {
                        return addr;
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
    
    public String getProviderTermForIpAddress(Locale locale) {
        return "IP address";
    }

    public boolean isAssigned(AddressType type) {
        return false;
    }
    
    public boolean isForwarding() {
        return true;
    }

    public boolean isRequestable(AddressType type) {
        return type.equals(AddressType.PUBLIC);
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(provider);
        
        try {
            method.get(method.buildUrl(Zones.LIST_ZONES, new Param[] { new Param("available", "true") }));
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
    
    public Iterable<org.dasein.cloud.network.IpAddress> listPrivateIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        return Collections.emptyList();
    }
    
    public Collection<org.dasein.cloud.network.IpAddress> listPublicIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        HashMap<String,LoadBalancer> loadBalancers = new HashMap<String,LoadBalancer>();

        for( LoadBalancer lb : provider.getNetworkServices().getLoadBalancerSupport().listLoadBalancers() ) {
            loadBalancers.put(lb.getProviderLoadBalancerId(), lb);
        }
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_PUBLIC_IP_ADDRESSES, new Param[] { new Param("zoneId", provider.getContext().getRegionId()) }));
        ArrayList<org.dasein.cloud.network.IpAddress> addresses = new ArrayList<org.dasein.cloud.network.IpAddress>();
        NodeList matches = doc.getElementsByTagName("publicipaddress");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            org.dasein.cloud.network.IpAddress addr = toAddress(matches.item(i), loadBalancers);
            
            if( addr != null && (!unassignedOnly || !addr.isAssigned()) ) {
                addresses.add(addr);
            }
        }
        return addresses;
    }

    public Collection<IpForwardingRule> listRules(String addressId) throws InternalException, CloudException {
        Logger logger = CloudstackProvider.getLogger(IpAddress.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + IpAddress.class.getName() + ".listRules('" + addressId + "')");
        }
        try {
            ArrayList<IpForwardingRule> rules = new ArrayList<IpForwardingRule>();
            Param[] params;
            
            if( provider.getVersion().greaterThan(CloudstackVersion.CS21) ) {
                params = new Param[] { new Param("ipaddressid", addressId) };                    
            }
            else {
                params = new Param[] { new Param("ipAddress", addressId) };
            }
            CloudstackMethod method = new CloudstackMethod(provider);
            Document doc = method.get(method.buildUrl(LIST_PORT_FORWARDING_RULES, params)); 
            
            if( doc == null ) {
                throw new CloudException("No such IP address: " + addressId);
            }
            NodeList matches = doc.getElementsByTagName("portforwardingrule");
            
            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);
                
                if( node != null ) {
                    IpForwardingRule rule = new IpForwardingRule();
                    NodeList list = node.getChildNodes();
    
                    rule.setAddressId(addressId);
                    for( int j=0; j<list.getLength(); j++ ) {
                        Node attr = list.item(j);
                        
                        if( attr.getNodeName().equals("publicport") ) {
                            rule.setPublicPort(Integer.parseInt(attr.getFirstChild().getNodeValue()));                        
                        }
                        else if( attr.getNodeName().equals("privateport") ) {
                            rule.setPrivatePort(Integer.parseInt(attr.getFirstChild().getNodeValue()));                        
                        }
                        else if( attr.getNodeName().equals("protocol") ) {
                            rule.setProtocol(Protocol.valueOf(attr.getFirstChild().getNodeValue().toUpperCase()));
                        } 
                        else if( attr.getNodeName().equals("id") ) {
                            rule.setProviderRuleId(attr.getFirstChild().getNodeValue());
                        } 
                        else if( attr.getNodeName().equals("virtualmachineid") ) {
                            rule.setServerId(attr.getFirstChild().getNodeValue().toUpperCase());
                        } 
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("listRules(): * " + rule);
                    }
                    rules.add(rule);
                }
            }
            return rules;
        }
        catch( RuntimeException e ) {
            logger.error("listRules(): Runtime exception listing rules for " + addressId + ": " + e.getMessage());
            if( logger.isTraceEnabled() ) {
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
        catch( Error e ) {
            logger.error("listRules(): Error listing rules for " + addressId + ": " + e.getMessage());
            if( logger.isTraceEnabled() ) {
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + IpAddress.class.getName() + ".listRules()");
            }
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    public void releaseFromPool(String addressId) throws InternalException, CloudException {
        Logger logger = CloudstackProvider.getLogger(IpAddress.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + IpAddress.class.getName() + ".releaseFromPool(" + addressId + ")");
        }
        try {
            CloudstackMethod method = new CloudstackMethod(provider);
        
            method.get(method.buildUrl(DISASSOCIATE_IP_ADDRESS, new Param[] { new Param(isId() ? "id" : "ipaddress", addressId) }));
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + IpAddress.class.getName() + ".releaseFromPool()");
            }
        }
    }
    
    public void releaseFromServer(String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException();
    }

    public String request(AddressType addressType) throws InternalException, CloudException {
        if( !addressType.equals(AddressType.PUBLIC) ) {
            throw new OperationNotSupportedException("Cannot request private addresses.");
        }
        CloudstackMethod method = new CloudstackMethod(provider);
        Document doc;

        //if( isBasic() ) {
            doc = method.get(method.buildUrl(ASSOCIATE_IP_ADDRESS,  new Param[] { new Param("zoneId", provider.getContext().getRegionId())  }));
       // }
        //else {
          //  throw new
            //doc = method.get(method.buildUrl(ASSOCIATE_IP_ADDRESS,  new Param[] { new Param("zoneId", provider.getContext().getRegionId())  }))
        //}
        NodeList matches;
        
        if( provider.getVersion().greaterThan(CloudstackVersion.CS21) ) {
            matches = doc.getElementsByTagName("id");
        }
        else {
            matches = doc.getElementsByTagName("ipaddress");
        }
        String id = null;
        if( matches.getLength() > 0 ) {
            id = matches.item(0).getFirstChild().getNodeValue();
        }
        if( id == null ) {
            throw new CloudException("Failed to request an IP address without error");
        }
        provider.waitForJob(doc, ASSOCIATE_IP_ADDRESS);
        return id;
    }
    
    public void stopForward(String ruleId) throws InternalException, CloudException {
        Logger logger = CloudstackProvider.getLogger(IpAddress.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + IpAddress.class.getName() + ".stopForward(" + ruleId + ")");
        }
        try {
            CloudstackMethod method = new CloudstackMethod(provider);
            Document doc = method.get(method.buildUrl(STOP_FORWARD, new Param[] { new Param("id", ruleId) }));

            provider.waitForJob(doc, STOP_FORWARD);
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + IpAddress.class.getName() + ".stopForward()");
            }            
        }
    }
    
    private org.dasein.cloud.network.IpAddress toAddress(Node node, Map<String,LoadBalancer> loadBalancers) throws InternalException, CloudException {
        org.dasein.cloud.network.IpAddress address = new org.dasein.cloud.network.IpAddress();
        NodeList attributes = node.getChildNodes();
        
        address.setRegionId(provider.getContext().getRegionId());
        address.setServerId(null);
        address.setProviderLoadBalancerId(null);
        address.setAddressType(AddressType.PUBLIC);
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node n = attributes.item(i);
            String name = n.getNodeName().toLowerCase();
            String value;
            
            if( n.getChildNodes().getLength() > 0 ) {
                value = n.getFirstChild().getNodeValue();
            }
            else {
                value = null;
            }
            if( name.equalsIgnoreCase("id") ) {
                address.setIpAddressId(value);
            }
            else if( name.equalsIgnoreCase("ipaddress") ) {
                if( address.getProviderIpAddressId() == null ) { // 2.1
                    address.setIpAddressId(value);
                }
                address.setAddress(value);
            }
            else if( name.equalsIgnoreCase("zoneid") ) {
                address.setRegionId(value);
            }
            else if( name.equalsIgnoreCase("virtualmachineid") ) {
                address.setServerId(value);
            }
            else if( name.equalsIgnoreCase("state") ) {
                if( value != null && !value.equalsIgnoreCase("allocated") ) {
                    return null;
                }
            }
        }
        if( loadBalancers != null ) {
            LoadBalancer lb = loadBalancers.get(address.getAddress());
            
            if( lb != null ) {
                address.setProviderLoadBalancerId(lb.getProviderLoadBalancerId());
            }
        }
        if( address.getServerId() == null ) {
            for( VirtualMachine vm : provider.getComputeServices().getVirtualMachineSupport().listVirtualMachines() ) {
                String[] addrs = vm.getPublicIpAddresses();
                
                if( addrs != null ) {
                    for( String addr : addrs ) {
                        if( addr.equals(address.getAddress()) ) {
                            address.setServerId(vm.getProviderVirtualMachineId());
                        }
                    }
                }
            }
        }
        return address;
    }
    
    private FirewallRule toRule(String ipAddressId, Node node) {
        if( node == null) {
            return null;
        }
        
        NodeList attributes = node.getChildNodes();
        FirewallRule rule = new FirewallRule();
        rule.setFirewallId(ipAddressId);
        rule.setPermission(Permission.ALLOW);
        rule.setDirection(Direction.INGRESS);
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
            if( name.equalsIgnoreCase("cidrlist") ) {
                rule.setCidr(value);
            }
            else if( name.equalsIgnoreCase("endport") ) {
                rule.setEndPort(Integer.parseInt(value));
            }
            else if( name.equalsIgnoreCase("startport") ) {
                rule.setStartPort(Integer.parseInt(value));
            }
            else if( name.equalsIgnoreCase("protocol") ) {
                rule.setProtocol(Protocol.valueOf(value.toUpperCase()));
            }
            else if( name.equalsIgnoreCase("id") ) {
                rule.setProviderRuleId(value);
            }
        }
        return rule;
    }
    
    /*
    private boolean hasRules(String address) throws InternalException, CloudException {
        return (provider.getNetworkServices().getLoadBalancerSupport().getLoadBalancer(address) != null);
        HttpClient client = new HttpClient();
        GetMethod get = new GetMethod();
        Document doc;
        int code;
        
        try {
            method.get(method.buildUrl(LoadBalancers.LIST_LOAD_BALANCER_RULES, new Param[] { new Param("publicIp", address) }));
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        }
        catch( SignatureException e ) {
            throw new InternalException("Unable to generate a valid signature: " + e.getMessage());
        }
        try {
            code = client.executeMethod(get);
        }
        catch( HttpException e ) {
            throw new InternalException("HttpException during GET: " + e.getMessage());
        }
        catch( IOException e ) {
            throw new CloudException("IOException during GET: " + e.getMessage());
        }
        if( code != HttpStatus.SC_OK ) {
            if( code == 401 ) {
                throw new CloudException("Unauthorized user");
            }
            else if( code == 430 ) {
                throw new InternalException("Malformed parameters");
            }
            else if( code == 431 ) {
                throw new InternalException("Invalid parameters");
            }
            else if( code == 530 || code == 570 ) {
                throw new CloudException("Server error in cloud (" + code + ")");
            }
            throw new CloudException("Received error code from server: " + code);
        }
        try {
            doc = provider.parseResponse(get.getResponseBodyAsStream());
        }
        catch( IOException e ) {
            throw new CloudException("IOException getting stream: " + e.getMessage());
        }
        
        NodeList rules = doc.getElementsByTagName("loadbalancerrule");
        return (rules.getLength() > 0);
    }
        */
}
