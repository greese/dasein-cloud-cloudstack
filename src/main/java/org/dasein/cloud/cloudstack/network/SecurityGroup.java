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
import java.util.Locale;

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
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.dasein.cloud.network.RuleTarget;
import org.dasein.cloud.network.RuleTargetType;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SecurityGroup implements FirewallSupport {
    static private final Logger logger = Logger.getLogger(SecurityGroup.class);

    static public final String AUTHORIZE_SECURITY_GROUP_EGRESS  = "authorizeSecurityGroupEgress";
    static public final String AUTHORIZE_SECURITY_GROUP_INGRESS = "authorizeSecurityGroupIngress";
    static public final String CREATE_SECURITY_GROUP            = "createSecurityGroup";
    static public final String DELETE_SECURITY_GROUP            = "deleteSecurityGroup";
    static public final String LIST_SECURITY_GROUPS             = "listSecurityGroups";
    static public final String REVOKE_SECURITY_GROUP_INGRESS    = "revokeSecurityGroupIngress";

    static private boolean isIP(@Nonnull String test) {
        String[] parts = test.split("\\.");

        if( parts.length != 4 ) {
            return false;
        }
        for( String part : parts ) {
            try {
                Integer x = Integer.parseInt(part);

                if( x < 0 || x > 255 ) {
                    return false;
                }
            }
            catch( NumberFormatException e ) {
                return false;
            }
        }
        return true;
    }

    private CSCloud cloudstack;
    
    SecurityGroup(CSCloud cloudstack) { this.cloudstack = cloudstack; }
    
    @Override
    @Deprecated
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, Direction.INGRESS, Permission.ALLOW, cidr, protocol, beginPort, endPort);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, direction, Permission.ALLOW, cidr, protocol, beginPort, endPort);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, direction, permission, cidr, protocol, RuleTarget.getCIDR(cidr), beginPort, endPort);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String source, @Nonnull Protocol protocol, @Nonnull RuleTarget target, int beginPort, int endPort) throws CloudException, InternalException {
        if( !permission.equals(Permission.ALLOW) ) {
            throw new OperationNotSupportedException("Only ALLOW arules are supported");
        }
        boolean group = false;

        if( source.indexOf('/') == -1 ) {
            // might be a security group
            if( isIP(source) ) {
                source = source + "/32";
            }
            else {
                group = true;
            }
        }
        Param[] params;
        CSMethod method = new CSMethod(cloudstack);

        if( group ) {
            throw new CloudException("Security group sources are not supported");
        }
        else {
            params = new Param[] { new Param("securitygroupid", firewallId), new Param("cidrlist", source), new Param("startport", String.valueOf(beginPort)), new Param("endport", String.valueOf(endPort)), new Param("protocol", protocol.name()) };
        }
        if( direction.equals(Direction.INGRESS) ) {
            method.get(method.buildUrl(AUTHORIZE_SECURITY_GROUP_INGRESS, params));
        }
        else {
            method.get(method.buildUrl(AUTHORIZE_SECURITY_GROUP_EGRESS, params));
        }

        for( FirewallRule rule : getRules(firewallId) ) {
            RuleTarget t = rule.getTarget();

            if( t.getRuleTargetType().equals(RuleTargetType.GLOBAL) && source.equals(rule.getSource()) ) {
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
        throw new OperationNotSupportedException("Firewalls may not be created for specified VLANs");
    }
    
    @Override
    public void delete(@Nonnull String firewallId) throws InternalException, CloudException {
        try {
            for( FirewallRule rule : getRules(firewallId) ) {
                try { revoke(rule.getProviderRuleId()); }
                catch( Throwable ignore ) { }
            }
        }
        catch( Throwable ignore ) {
            // ignore
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
        matches = doc.getElementsByTagName("egressrule");
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

    @Override
    public @Nonnull Iterable<ResourceStatus> listFirewallStatus() throws InternalException, CloudException {
        ProviderContext ctx = cloudstack.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        CSMethod method = new CSMethod(cloudstack);
        Document doc = method.get(method.buildUrl(LIST_SECURITY_GROUPS));
        ArrayList<ResourceStatus> firewalls = new ArrayList<ResourceStatus>();
        NodeList matches = doc.getElementsByTagName("securitygroup");

        for( int i=0; i<matches.getLength(); i++ ) {
            Node node = matches.item(i);

            if( node != null ) {
                ResourceStatus fw = toStatus(node);

                if( fw != null ) {
                    firewalls.add(fw);
                }
            }
        }
        return firewalls;
    }

    @Override
    public @Nonnull Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        return Collections.singletonList(RuleTargetType.GLOBAL);
    }

    @Override
    public void revoke(@Nonnull String providerFirewallRuleId) throws InternalException, CloudException {
        Param[] params = new Param[] { new Param("id", providerFirewallRuleId) };
        CSMethod method = new CSMethod(cloudstack);

        method.get(method.buildUrl(REVOKE_SECURITY_GROUP_INGRESS, params));
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
        revoke(firewallId, Direction.INGRESS, Permission.ALLOW, cidr, protocol, RuleTarget.getGlobal(), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, Permission.ALLOW, cidr, protocol, RuleTarget.getGlobal(), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String source, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, permission, source, protocol, RuleTarget.getGlobal(), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String source, @Nonnull Protocol protocol, @Nonnull RuleTarget target, int beginPort, int endPort) throws CloudException, InternalException {
        if( !Permission.ALLOW.equals(permission) ) {
            throw new OperationNotSupportedException("Only ALLOW rules are supported");
        }
        FirewallRule rule = null;

        for( FirewallRule r : getRules(firewallId) ) {
            if( r.getTarget().getRuleTargetType().equals(target.getRuleTargetType()) ) {
                if( r.getDirection().equals(direction) ) {
                    if( source.equals(r.getSource()) ) {
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
            }
        }
        if( rule == null ) {
            logger.warn("No such rule for " + firewallId + ": " + direction + "/" + permission + "/" + source + "/" + protocol + "/" + beginPort + "/" + endPort);
            return;
        }
        revoke(rule.getProviderRuleId());
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        return (!inVlan && permission.equals(Permission.ALLOW));
    }

    @Override
    public boolean supportsFirewallSources() throws CloudException, InternalException {
        return false;
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
        int startPort = -1, endPort = -1;
        Protocol protocol = Protocol.TCP;
        Direction direction = Direction.INGRESS;
        String source = "0.0.0.0/0";
        String ruleId = null;

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
                source = value;
            }
            else if( name.equalsIgnoreCase("endport") && value != null ) {
                endPort = Integer.parseInt(value);
            }
            else if( name.equalsIgnoreCase("startport") && value != null ) {
                startPort = Integer.parseInt(value);
            }
            else if( name.equalsIgnoreCase("protocol") && value != null ) {
                protocol = Protocol.valueOf(value.toUpperCase());
            }
            else if( name.equalsIgnoreCase("ruleId") && value != null ) {
                ruleId = value;
            }
        }
        if( (startPort == -1 || endPort == -1) && (startPort != -1 || endPort != -1) ) {
            if( startPort == -1 ) {
                startPort = endPort;
            }
            else {
                endPort = startPort;
            }
        }
        return FirewallRule.getInstance(ruleId, firewallId, source, direction, protocol, Permission.ALLOW, RuleTarget.getGlobal(), startPort, endPort);
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();

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
                return new ResourceStatus(value, true);
            }
        }
        return null;
    }
}
