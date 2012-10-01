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
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
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
import javax.annotation.Nullable;

public class SecurityGroup implements FirewallSupport {
    static private final Logger logger = Logger.getLogger(SecurityGroup.class);
    
    static public final String AUTHORIZE_SECURITY_GROUP_INGRESS = "authorizeSecurityGroupIngress";
    static public final String CREATE_SECURITY_GROUP            = "createSecurityGroup";
    static public final String DELETE_SECURITY_GROUP            = "deleteSecurityGroup";
    static public final String LIST_SECURITY_GROUPS             = "listSecurityGroups";
    static public final String REVOKE_SECURITY_GROUP_INGRESS    = "revokeSecurityGroupIngress";
    
    private CSCloud cloudstack;
    
    SecurityGroup(CSCloud cloudstack) { this.cloudstack = cloudstack; }
    
    @Override
    @Deprecated
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, Direction.INGRESS, cidr, protocol, beginPort, endPort);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        if( direction.equals(Direction.INGRESS) ) {
            Param[] params = new Param[] { new Param("securitygroupid", firewallId), new Param("cidrlist", cidr), new Param("startport", String.valueOf(beginPort)), new Param("endport", String.valueOf(endPort)), new Param("protocol", protocol.name()) };
            CSMethod method = new CSMethod(cloudstack);

            method.get(method.buildUrl(AUTHORIZE_SECURITY_GROUP_INGRESS, params));
            for( FirewallRule rule : getRules(firewallId) ) {
                if( cidr.equals(rule.getCidr()) ) {
                    if( protocol.equals(rule.getProtocol()) ) {
                        if( rule.getStartPort() == beginPort ) {
                            if( rule.getEndPort() == endPort ) {
                                return rule.getProviderRuleId();
                            }
                        }
                    }
                }
            }
            throw new CloudException("Unable to identify newly created firewall rule ID");
        }
        throw new OperationNotSupportedException("No egress rules are supported");
    }

    @Override
    public @Nonnull String create(@Nonnull String name, @Nonnull String description) throws InternalException, CloudException {
        Param[] params = new Param[] { new Param("name", name), new Param("description", description) };
        CSMethod method = new CSMethod(cloudstack);
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
    public @Nonnull String createInVLAN(@Nonnull String name, @Nonnull String description, @Nonnull String providerVlanId) throws InternalException, CloudException {
        /*
        Param[] params = new Param[] { new Param("name", name), new Param("description", description) };
        CSMethod method = new CSMethod(cloudstack);
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
              */
        throw new OperationNotSupportedException("Firewalls may not be created for specified VLANs");
    }
    
    @Override
    public void delete(@Nonnull String firewallId) throws InternalException, CloudException {
        for( FirewallRule rule : getRules(firewallId) ) {
            String cidr = rule.getCidr();
            Protocol p = rule.getProtocol();
            Direction d = rule.getDirection();

            revoke(firewallId, d == null ? Direction.INGRESS : d, cidr == null ? "0.0.0.0/0" : cidr, p == null ? Protocol.TCP : p, rule.getStartPort(), rule.getEndPort());
        }
        CSMethod method = new CSMethod(cloudstack);
        
        method.get(method.buildUrl(DELETE_SECURITY_GROUP, new Param("id", firewallId)));
    }

    @Override
    public @Nullable Firewall getFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
        ProviderContext ctx = cloudstack.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(cloudstack);
        
        try {
            Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param("id", firewallId)));
            NodeList matches = doc.getElementsByTagName("securitygroup");
            
            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);
                
                if( node != null ) {
                    Firewall fw = toFirewall(node, ctx);
                
                    if( fw != null ) {
                        return fw;
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

    @Override
    public @Nonnull String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "security group";
    }

    @Override
    public @Nonnull Collection<FirewallRule> getRules(@Nonnull String firewallId) throws InternalException, CloudException {
        CSMethod method = new CSMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param("id", firewallId)));
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
        ProviderContext ctx = cloudstack.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        return cloudstack.getDataCenterServices().supportsSecurityGroups(regionId, false);
    }
    
    @Override
    public @Nonnull Collection<Firewall> list() throws InternalException, CloudException {
        ProviderContext ctx = cloudstack.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS));
        ArrayList<Firewall> firewalls = new ArrayList<Firewall>();
        NodeList matches = doc.getElementsByTagName("securitygroup");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                Firewall fw = toFirewall(node, ctx);
            
                if( fw != null ) {
                    firewalls.add(fw);
                }
            }
        }
        return firewalls;
    }


    public @Nonnull Iterable<String> listFirewallsForVM(@Nonnull String vmId) throws CloudException, InternalException {
        ProviderContext ctx = cloudstack.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS, new Param("virtualmachineId", vmId)));
        ArrayList<String> firewalls = new ArrayList<String>();
        NodeList matches = doc.getElementsByTagName("securitygroup");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);
            
            if( node != null ) {
                Firewall fw = toFirewall(node, ctx);
            
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
    public void revoke(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, Direction.INGRESS, cidr, protocol, beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {        FirewallRule rule = null;
        if( !Direction.INGRESS.equals(direction) ) {
            throw new OperationNotSupportedException("Only ingress rules are supported");
        }
        for( FirewallRule r : getRules(firewallId) ) {
            if( cidr.equals(r.getCidr()) ) {
                if( protocol.equals(r.getProtocol()) ) {
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
        CSMethod method = new CSMethod(cloudstack);

        method.get(method.buildUrl(REVOKE_SECURITY_GROUP_INGRESS, params));
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, boolean inVlan) throws CloudException, InternalException {
        return (Direction.INGRESS.equals(direction) && !inVlan);
    }

    private @Nullable Firewall toFirewall(@Nullable Node node, @Nonnull ProviderContext ctx) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was specified for this request");
        }
        NodeList attributes = node.getChildNodes();
        Firewall firewall = new Firewall();
        
        firewall.setActive(true);
        firewall.setAvailable(true);
        firewall.setRegionId(regionId);
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
            if( name.equalsIgnoreCase("id") && value != null ) {
                firewall.setProviderFirewallId(value);
            }
            else if( name.equalsIgnoreCase("description") && value != null ) {
                firewall.setDescription(value);
            }
            else if( name.equalsIgnoreCase("name") && value != null ) {
                firewall.setName(value);
            }
        }
        if( firewall.getProviderFirewallId() == null ) {
            logger.warn("Discovered firewall " + firewall.getProviderFirewallId() + " with an empty firewall ID");
            return null;
        }
        String id = firewall.getProviderFirewallId();
        String name;

        if( id == null ) {
            return null;
        }
        name = firewall.getName();
        if( name == null ) {
            name = id;
            firewall.setName(name);
        }
        if( firewall.getDescription() == null ) {
            firewall.setDescription(name);
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
        rule.setCidr("0.0.0.0/0");
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
            if( name.equalsIgnoreCase("cidr") && value != null ) {
                rule.setCidr(value);
            }
            else if( name.equalsIgnoreCase("endport") && value != null ) {
                rule.setEndPort(Integer.parseInt(value));
            }
            else if( name.equalsIgnoreCase("startport") && value != null ) {
                rule.setStartPort(Integer.parseInt(value));
            }
            else if( name.equalsIgnoreCase("protocol") && value != null ) {
                rule.setProtocol(Protocol.valueOf(value.toUpperCase()));
            }
            else if( name.equalsIgnoreCase("ruleId") && value != null ) {
                rule.setProviderRuleId(value);
            }
        }
        return rule;
    }
}
