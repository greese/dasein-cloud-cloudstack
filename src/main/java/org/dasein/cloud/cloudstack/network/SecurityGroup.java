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
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.cloudstack.CloudstackException; 
import org.dasein.cloud.cloudstack.CloudstackMethod;
import org.dasein.cloud.cloudstack.CloudstackProvider;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;

public class SecurityGroup implements FirewallSupport {
    static private final Logger logger = Logger.getLogger(SecurityGroup.class);
    
    static public final String AUTHORIZE_SECURITY_GROUP_INGRESS = "authorizeSecurityGroupIngress";
    static public final String CREATE_SECURITY_GROUP            = "createSecurityGroup";
    static public final String DELETE_SECURITY_GROUP            = "deleteSecurityGroup";
    static public final String LIST_SECURITY_GROUPS             = "listSecurityGroups";
    static public final String REVOKE_SECURITY_GROUP_INGRESS    = "revokeSecurityGroupIngress";
    
    private CloudstackProvider cloudstack;
    
    SecurityGroup(CloudstackProvider cloudstack) { this.cloudstack = cloudstack; }
    
    @Override
    public String authorize(String firewallId, String cidr, Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        Param[] params = new Param[] { new Param("securitygroupid", firewallId), new Param("cidrlist", cidr), new Param("startport", String.valueOf(beginPort)), new Param("endport", String.valueOf(endPort)), new Param("protocol", protocol.name()) };
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        
        method.get(method.buildUrl(AUTHORIZE_SECURITY_GROUP_INGRESS, params));
        for( FirewallRule rule : getRules(firewallId) ) {
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

    @Override
    public String create(String name, String description) throws InternalException, CloudException {
        Param[] params = new Param[] { new Param("name", name), new Param("description", description) };
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(CREATE_SECURITY_GROUP, params));
        NodeList matches = doc.getElementsByTagName("id");
        String groupId = null;
        
        if( matches.getLength() > 0 ) {
            groupId = matches.item(0).getFirstChild().getNodeValue();
        }
        if( groupId == null ) {
            throw new CloudException("Failed to create firewall");
        }
        return groupId;
    }

    @Override
    public String createInVLAN(String name, String description, String providerVlanId) throws InternalException, CloudException {
        Param[] params = new Param[] { new Param("name", name), new Param("description", description) };
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(CREATE_SECURITY_GROUP, params));
        NodeList matches = doc.getElementsByTagName("id");
        String groupId = null;
        
        if( matches.getLength() > 0 ) {
            groupId = matches.item(0).getFirstChild().getNodeValue();
        }
        if( groupId == null ) {
            throw new CloudException("Failed to create firewall");
        }
        return groupId;        
    }
    
    @Override
    public void delete(String firewallId) throws InternalException, CloudException {
        for( FirewallRule rule : getRules(firewallId) ) {
            revoke(firewallId, rule.getCidr(), rule.getProtocol(), rule.getStartPort(), rule.getEndPort());
        }
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        
        method.get(method.buildUrl(DELETE_SECURITY_GROUP, new Param[] { new Param("id", firewallId) }));
    }

    @Override
    public Firewall getFirewall(String firewallId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        
        try {
            Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param[] {  new Param("id", firewallId) }));
            NodeList matches = doc.getElementsByTagName("securitygroup");
            
            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);
                
                if( node != null ) {
                    Firewall fw = toFirewall(node);
                
                    if( fw != null ) {
                        return fw;
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

    @Override
    public String getProviderTermForFirewall(Locale locale) {
        return "security group";
    }

    @Override
    public Collection<FirewallRule> getRules(String firewallId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param[] {  new Param("id", firewallId) }));
        ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();
        
        NodeList matches = doc.getElementsByTagName("ingressrule");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                FirewallRule rule = toRule(firewallId, node);
                
                if( rule != null ) {
                    rules.add(rule);
                }
            }
        }
        return rules;
    }

    public boolean isSubscribed() throws CloudException, InternalException {
        return cloudstack.getDataCenterServices().supportsSecurityGroups(cloudstack.getContext().getRegionId(), false);
    }
    
    @Override
    public Collection<Firewall> list() throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param[] {  }));
        ArrayList<Firewall> firewalls = new ArrayList<Firewall>();
        NodeList matches = doc.getElementsByTagName("securitygroup");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                Firewall fw = toFirewall(node);
            
                if( fw != null ) {
                    firewalls.add(fw);
                }
            }
        }
        return firewalls;
    }


    public Iterable<String> listFirewallsForVM(String vmId) throws CloudException, InternalException {
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param[] { new Param("virtualmachineId", vmId) }));
        ArrayList<String> firewalls = new ArrayList<String>();
        NodeList matches = doc.getElementsByTagName("securitygroup");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                Firewall fw = toFirewall(node);
            
                if( fw != null ) {
                    firewalls.add(fw.getProviderFirewallId());
                }
            }
        }
        return firewalls;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void revoke(String firewallId, String cidr, Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        FirewallRule rule = null;
        
        for( FirewallRule r : getRules(firewallId) ) {
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
            logger.warn("No such rule for " + firewallId + ": " + cidr + "/" + protocol + "/" + beginPort + "/" + endPort);
            return;
        }
        Param[] params = new Param[] { new Param("id", rule.getProviderRuleId()) };
        CloudstackMethod method = new CloudstackMethod(cloudstack);
        
        method.get(method.buildUrl(REVOKE_SECURITY_GROUP_INGRESS, params));
           
    }

    private Firewall toFirewall(Node node) {
        if( node == null ) {
            return null;
        }
        
        NodeList attributes = node.getChildNodes();
        Firewall firewall = new Firewall();
        
        firewall.setActive(true);
        firewall.setAvailable(true);
        firewall.setRegionId(cloudstack.getContext().getRegionId());
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
                firewall.setProviderFirewallId(value);
            }
            else if( name.equalsIgnoreCase("description") ) {
                firewall.setDescription(value);
            }
            else if( name.equalsIgnoreCase("name") ) {
                firewall.setName(value);
            }
        }
        if( firewall.getProviderFirewallId() == null ) {
            logger.warn("Discovered firewall " + firewall.getProviderFirewallId() + " with an empty firewall ID");
            return null;
        }
        if( firewall.getName() == null ) {
            firewall.setName(firewall.getProviderFirewallId());
        }
        if( firewall.getDescription() == null ) {
            firewall.setDescription(firewall.getName());
        }
        return firewall;
    }
    
    private FirewallRule toRule(String firewallId, Node node) {
        if( node == null) {
            return null;
        }
        
        NodeList attributes = node.getChildNodes();
        FirewallRule rule = new FirewallRule();
        rule.setFirewallId(firewallId);
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
            if( name.equalsIgnoreCase("cidr") ) {
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
            else if( name.equalsIgnoreCase("ruleId") ) {
                rule.setProviderRuleId(value);
            }
        }
        return rule;
    }
}
