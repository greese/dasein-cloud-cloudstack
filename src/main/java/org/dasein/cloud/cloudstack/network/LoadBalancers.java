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

package org.dasein.cloud.cloudstack.network;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.CSVersion;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class LoadBalancers extends AbstractLoadBalancerSupport<CSCloud> {
    static public final String ASSIGN_TO_LOAD_BALANCER_RULE       = "assignToLoadBalancerRule";
    static public final String CREATE_LOAD_BALANCER_RULE          = "createLoadBalancerRule";
    static public final String DELETE_LOAD_BALANCER_RULE          = "deleteLoadBalancerRule";
    static public final String LIST_LOAD_BALANCER_RULES           = "listLoadBalancerRules";
    static public final String LIST_LOAD_BALANCER_RULE_INSTANCES  = "listLoadBalancerRuleInstances";
    static public final String REMOVE_FROM_LOAD_BALANCER_RULE     = "removeFromLoadBalancerRule";
    static public final String UPLOAD_SSL_CERTIFICATE             = "uploadSslCert";
    public static final String LIST_SSL_CERTIFICATES              = "listSslCerts";
    public static final String DELETE_SSL_CERTIFICATE             = "deleteSslCert";
    public static final String CREATE_LB_HEALTH_CHECK_POLICY      = "createLBHealthCheckPolicy";

    protected LoadBalancers(CSCloud provider) {
        super(provider);
    }

    @Override
    public void addServers(@Nonnull String toLoadBalancerId, @Nonnull String ... serverIds) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.addServers");
        try {
            try {
                LoadBalancer lb = getLoadBalancer(toLoadBalancerId);

                if( lb == null ) {
                    throw new CloudException("No such load balancer: " + toLoadBalancerId);
                }
                if( serverIds == null || serverIds.length == 0 ) {
                    return;
                }
                for( LbListener listener : lb.getListeners() ) {
                    String ruleId = getVmOpsRuleId(listener.getAlgorithm(), toLoadBalancerId, listener.getPublicPort(), listener.getPrivatePort(), lb.getProviderVlanId());
                    StringBuilder str = new StringBuilder();

                    for( int i=0; i<serverIds.length; i++ ) {
                        str.append(serverIds[i]);
                        if( i < serverIds.length-1 ) {
                            str.append(",");
                        }
                    }
                    Document doc = new CSMethod(getProvider()).get(ASSIGN_TO_LOAD_BALANCER_RULE, new Param("id", ruleId), new Param("virtualMachineIds", str.toString()));

                    getProvider().waitForJob(doc, "Add Server");
                }
            }
            catch( RuntimeException e ) {
                throw new InternalException(e);
            }
            catch( Error e ) {
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createLoadBalancer(@Nonnull LoadBalancerCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.create");
        try {
            @SuppressWarnings("ConstantConditions") org.dasein.cloud.network.IpAddress publicAddress = getProvider().getNetworkServices().getIpAddressSupport().getIpAddress(options.getProviderIpAddressId());

            if( publicAddress == null ) {
                throw new CloudException("You must specify the IP address for your load balancer.");
            }
            for( LbListener listener : options.getListeners() ) {
                if( !isId(options.getProviderIpAddressId()) ) {
                    createVmOpsRule(options.getName(), listener.getAlgorithm(), options.getProviderIpAddressId(), listener.getPublicPort(), listener.getPrivatePort(), options.getProviderVlanId());
                }
                else {
                    createCloudstack22Rule(options.getName(), listener.getAlgorithm(), options.getProviderIpAddressId(), listener.getPublicPort(), listener.getPrivatePort());
                }
            }
            for( LoadBalancerEndpoint endpoint : options.getEndpoints() ) {
                if( endpoint.getEndpointType().equals(LbEndpointType.VM) ) {
                    addServers(publicAddress.getRawAddress().getIpAddress(), endpoint.getEndpointValue());
                }
            }
            
            String lbId = getRuleId(publicAddress.getRawAddress().getIpAddress());

            // Set tags
            List<Tag> tags = new ArrayList<Tag>();
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
            getProvider().createTags(new String[] { lbId }, "LoadBalancer", tags.toArray(new Tag[tags.size()]));
            return publicAddress.getRawAddress().getIpAddress();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public LoadBalancerHealthCheck createLoadBalancerHealthCheck( @Nonnull HealthCheckOptions options ) throws CloudException, InternalException {
        // TODO: add trace
        final List<Param> params = new ArrayList<Param>();
        final String ruleId = getRuleId(options.getProviderLoadBalancerId());
        params.add(new Param("lbruleid", ruleId));
        if( options.getDescription() != null ) {
            params.add(new Param("description", options.getDescription()));
        }
        params.add(new Param("healthythreshold", String.valueOf(options.getHealthyCount())));
        params.add(new Param("unhealthythreshold", String.valueOf(options.getUnhealthyCount())));
        params.add(new Param("intervaltime", String.valueOf(options.getInterval())));
        params.add(new Param("responsetimeout", String.valueOf(options.getTimeout())));
        params.add(new Param("pingpath", options.getHost() + ":" + options.getPort() + "/" + options.getPath()));

        final Document doc = new CSMethod(getProvider()).get(CREATE_LB_HEALTH_CHECK_POLICY, params);
        NodeList matches = doc.getElementsByTagName("healthcheckpolicy");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node n = matches.item(i);
            NodeList attributes = n.getChildNodes();
            String lbhcId = null;
            LoadBalancerHealthCheck.HCProtocol protocol = null;
            String path = null;
            int port = 0;
            int interval = 0;
            int healthyCount = 0;
            int unhealthyCount = 0;
            int timeout = 0;

            for( int j = 0; j < attributes.getLength(); j++ ) {
                Node child = attributes.item(j);
                String name = child.getNodeName().toLowerCase();
                String value;

                if( child.getChildNodes().getLength() > 0 ) {
                    value = child.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( "id".equalsIgnoreCase(name) ) {
                    lbhcId = value;
                }
                else if( "path".equalsIgnoreCase(name) ) {
                    path = value;
                }
                else if( "healthcheckinterval".equalsIgnoreCase(name) ) {
                    interval = Integer.parseInt(value);
                }
                else if( "healthcheckthresshold".equalsIgnoreCase(name) ) {
                    healthyCount = Integer.parseInt(value);
                }
                else if( "unhealthcheckthresshold".equalsIgnoreCase(name) ) {
                    unhealthyCount = Integer.parseInt(value);
                }
                else if( "pingpath".equalsIgnoreCase(name) ) {
                    path = value;
                }
                else if( "responsetime".equalsIgnoreCase(name) ) {
                    timeout = Integer.parseInt(value);
                }

            }
            return LoadBalancerHealthCheck.getInstance(lbhcId, protocol, port, path, interval, timeout, healthyCount, unhealthyCount);
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public @Nonnull String create(@Nonnull String name, @Nonnull String description, @Nullable String addressId, @Nullable String[] zoneIds, @Nullable LbListener[] listeners, @Nullable String[] serverIds, @Nullable String[] subnetIds, LbType type) throws CloudException, InternalException {
        // TODO: this should be declared via capabilities
        if( addressId == null ) {
            throw new CloudException("You must specify an IP address for load balancer creation");
        }
        LoadBalancerCreateOptions options = LoadBalancerCreateOptions.getInstance(name, description, addressId);

        if( zoneIds != null && zoneIds.length > 0 ) {
            options.limitedTo(zoneIds);
        }
        if( listeners != null && listeners.length > 0 ) {
            options.havingListeners(listeners);
        }
        if( serverIds != null && serverIds.length > 0 ) {
            options.withVirtualMachines(serverIds);
        }
        if( subnetIds != null && subnetIds.length > 0 ) {
            options.withProviderSubnetIds(subnetIds);
        }
        if (type != null) {
            options.asType(type);
        }
        return createLoadBalancer(options);
    }

    private void createVmOpsRule( String lbName, LbAlgorithm algorithm, String publicIp, int publicPort, int privatePort, String providerVlanId ) throws CloudException, InternalException {
        // TODO: add trace
        String id = getVmOpsRuleId(algorithm, publicIp, publicPort, privatePort, providerVlanId);
        
        if( id != null ) {
            return;
        }
        final List<Param> params = new ArrayList<Param>();


        params.add(new Param("publicIp", publicIp));
        params.add(new Param("publicPort", String.valueOf(publicPort)));
        params.add(new Param("privatePort", String.valueOf(privatePort)));
        params.add(new Param("algorithm", toAlgorithmString(algorithm)));
        if( providerVlanId != null ) {
//            params.add(new Param("networkid", providerVlanId));
        }
        if (lbName != null && !lbName.equals("")) {
            params.add(new Param("name", lbName));
        }
        else {
            params.add(new Param("name", "dsnlb_" + publicIp + "_" + publicPort + "_" + privatePort));
        }
        params.add(new Param("description", "dsnlb_" + publicIp + "_" + publicPort + "_" + privatePort));
        
        final Document doc = new CSMethod(getProvider()).get(CREATE_LOAD_BALANCER_RULE, params);
        NodeList matches = doc.getElementsByTagName("loadbalancerrule"); // v2.1
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Map<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
            Collection<LoadBalancer> lbs;
            Node node = matches.item(i);
            
            toRule(node, current);
            lbs = current.values();
            if( lbs.size() > 0 ) {
                return;
            }
        }
        matches = doc.getElementsByTagName("loadbalancer"); // v2.2
        for( int i=0; i<matches.getLength(); i++ ) {
            Map<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
            Collection<LoadBalancer> lbs;
            Node node = matches.item(i);
            
            toRule(node, current);
            lbs = current.values();
            if( lbs.size() > 0 ) {
                return;
            }
        }
        throw new CloudException("Failed to add load balancer rule (2).");
    }

    private String toAlgorithmString(LbAlgorithm algorithm) {
        switch( algorithm ) {
            case LEAST_CONN: return "leastconn";
            case SOURCE: return "source";
            default: return "roundrobin";
        }
    }

    private void createCloudstack22Rule(String lbName, LbAlgorithm algorithm, String publicIpId, int publicPort, int privatePort) throws CloudException, InternalException {
        // TODO: add trace
        String id = getVmOpsRuleId(algorithm, publicIpId, publicPort, privatePort, null);
        
        if( id != null ) {
            return;
        }

        final List<Param> params = new ArrayList<Param>();
        params.add(new Param("publicIpId", publicIpId));
        params.add(new Param("publicPort", String.valueOf(publicPort)));
        params.add(new Param("privatePort", String.valueOf(privatePort)));
        params.add(new Param("algorithm", toAlgorithmString(algorithm)));
        if (lbName != null && !lbName.equals("")) {
            params.add(new Param("name", lbName));
        }
        else {
            params.add(new Param("name", "dsnlb_" + publicIpId + "_" + publicPort + "_" + privatePort));
        }
        params.add(new Param("description", "dsnlb_" + publicIpId + "_" + publicPort + "_" + privatePort));

        final Document doc = new CSMethod(getProvider()).get(CREATE_LOAD_BALANCER_RULE, params);
        NodeList matches = doc.getElementsByTagName("loadbalancerrule"); // v2.1
        
        for( int i=0; i<matches.getLength(); i++ ) {
            Map<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
            Collection<LoadBalancer> lbs;
            Node node = matches.item(i);
            
            toRule(node, current);
            lbs = current.values();
            if( lbs.size() > 0 ) {
                return;
            }
        }
        matches = doc.getElementsByTagName("loadbalancer"); // v2.2.0 - v2.2.10
        for( int i=0; i<matches.getLength(); i++ ) {
            Map<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
            Collection<LoadBalancer> lbs;
            Node node = matches.item(i);
            
            toRule(node, current);
            lbs = current.values();
            if( lbs.size() > 0 ) {
                return;
            }
        }
        
        matches = doc.getElementsByTagName("jobid"); // v2.2.11 - v2.2.13
        if (matches.getLength() > 0) {
        	getProvider().waitForJob(doc, "Create Load Balancer Rule");
        	return;
        }
        
        throw new CloudException("Failed to add load balancer rule (2).");
    }
    
    @Override
    public @Nonnull LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    private transient volatile LBCapabilities capabilities;
    @Nonnull
    @Override
    public LoadBalancerCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new LBCapabilities(getProvider());
        }
        return capabilities;
    }

    private boolean isId(String ipAddressIdCandidate) {
        String[] parts = ipAddressIdCandidate.split("\\.");
        return (parts == null || parts.length != 4 );
    }
    
    @Override
    public @Nullable LoadBalancer getLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.getLoadBalancer");
        try {
            try {
                Map<String,LoadBalancer> matches = new HashMap<String,LoadBalancer>();
                boolean isId = isId(loadBalancerId);
                String key = (isId ? "publicIpId" : "publicIp");

                final Document doc = new CSMethod(getProvider()).get(LIST_LOAD_BALANCER_RULES, new Param(key, loadBalancerId));
                NodeList rules = doc.getElementsByTagName("loadbalancerrule");

                for( int i=0; i<rules.getLength(); i++ ) {
                    Node node = rules.item(i);

                    toRule(node, matches);
                }
                return matches.get(loadBalancerId);
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

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 0;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listLoadBalancerStatus");
        try {
            Map<String,LoadBalancer> matches = new HashMap<String,LoadBalancer>();
            CSMethod method = new CSMethod(getProvider());

            try {
                Document doc = method.get(LIST_LOAD_BALANCER_RULES);

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
                        doc = method.get(LIST_LOAD_BALANCER_RULES, new Param("pagesize", "500"), new Param("page", nextPage));
                    }
                    NodeList rules = doc.getElementsByTagName("loadbalancerrule");

                    for( int i=0; i<rules.getLength(); i++ ) {
                        Node node = rules.item(i);

                        toRule(node, matches);
                    }
                }
                final List<ResourceStatus> results = new ArrayList<ResourceStatus>();
                for( LoadBalancer lb : matches.values() ) {
                    if( matchesRegion(lb.getProviderLoadBalancerId()) ) {
                        results.add(new ResourceStatus(lb.getProviderLoadBalancerId(), lb.getCurrentState()));
                    }
                }
                return results;
            }
            catch( CloudException e ) {
                if( e.getHttpCode() == HttpServletResponse.SC_NOT_FOUND ) {
                    return Collections.emptyList();
                }
                e.printStackTrace();
                throw e;
            }
        }
        finally {
            APITrace.end();
        }
    }

    static private volatile List<LbAlgorithm> algorithms = null;
    
    private @Nonnull Collection<String> getServersAt(String ruleId) throws InternalException, CloudException {
        // TODO: add trace
        final List<String> ids = new ArrayList<String>();
        final CSMethod method = new CSMethod(getProvider());
        Document doc = method.get(LIST_LOAD_BALANCER_RULE_INSTANCES, new Param("id", ruleId));

        int numPages = 1;
        NodeList nodes = doc.getElementsByTagName("count");
        Node nd = nodes.item(0);
        if (nd != null) {
            String value = nd.getFirstChild().getNodeValue().trim();
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
                doc = method.get(LIST_LOAD_BALANCER_RULE_INSTANCES, new Param("id", ruleId), new Param("pagesize", "500"), new Param("page", nextPage));
            }
            NodeList instances = doc.getElementsByTagName("loadbalancerruleinstance");
        
            for( int i=0; i<instances.getLength(); i++ ) {
                Node node = instances.item(i);
                NodeList attributes = node.getChildNodes();

                for( int j=0; j<attributes.getLength(); j++ ) {
                    Node n = attributes.item(j);

                    if( n.getNodeName().equals("id") ) {
                        ids.add(n.getFirstChild().getNodeValue());
                    }
                }
            }
        }
        return ids;
    }
    
    private @Nullable String getVmOpsRuleId(@Nonnull LbAlgorithm lbAlgorithm, @Nonnull String publicIp, int publicPort, int privatePort, @Nullable String networkId) throws CloudException, InternalException {
        // TODO: add trace
        String ruleId = null;
        boolean isId = isId(publicIp);
        String key = (isId ? "publicIpId" : "publicIp");

        CSMethod method = new CSMethod(getProvider());
        final List<Param> parameters = new ArrayList<Param>();
        parameters.add(new Param(key, publicIp));
        if( networkId != null ) {
            parameters.add(new Param("networkid", networkId));
        }
        Document doc = method.get(LIST_LOAD_BALANCER_RULES, parameters);
        NodeList rules = doc.getElementsByTagName("loadbalancerrule");
        
        for( int i=0; i<rules.getLength(); i++ ) {
            Node node = rules.item(i);
            NodeList attributes = node.getChildNodes();
            boolean isIt = true;
            String id = null;
            
            for( int j=0; j<attributes.getLength(); j++ ) {
                Node n = attributes.item(j);
                String name = n.getNodeName().toLowerCase();
                String value;
                
                if( n.getChildNodes().getLength() > 0 ) {
                    value = n.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( name.equals("publicip") ) {
                    if( value != null && !value.equals(publicIp) ) {
                        isIt = false;
                        break;
                    }
                }
                else if( name.equals("publicport") ) {
                    if( value == null || publicPort != Integer.parseInt(value) ) {
                        isIt = false;
                        break;
                    }
                }
                else if( name.equals("privateport") ) {
                    if( value == null || privatePort != Integer.parseInt(value) ) {
                        isIt = false;
                        break;
                    }
                }
                else if( name.equals("algorithm") ) {
                    if( value == null || !value.equals(toAlgorithmString(lbAlgorithm)) ) {
                        isIt = false;
                        break;
                    }
                }
                else if( name.equals("id") ) {
                    id = value;
                }
            }
            if( isIt ) {
                ruleId = id;
                break;
            }
        }
        return ruleId;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.isSubscribed");
        try {
            new CSMethod(getProvider()).get(LIST_LOAD_BALANCER_RULES);
            return true;
        }
        catch( CSException e ) {
            int code = e.getHttpCode();

            if( code == HttpServletResponse.SC_FORBIDDEN || code == 401 || code == 531 ) {
                return false;
            }
            throw e;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancerEndpoint> listEndpoints(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listEndpoints");
        try {
            LoadBalancer lb = getLoadBalancer(loadBalancerId);

            if( lb == null ) {
                return Collections.emptyList();
            }
            final List<LoadBalancerEndpoint> endpoints = new ArrayList<LoadBalancerEndpoint>();

            //noinspection deprecation
            for( String serverId : lb.getProviderServerIds() ) {
                VirtualMachine vm = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);

                endpoints.add(LoadBalancerEndpoint.getInstance(LbEndpointType.VM, serverId, vm != null && vm.getCurrentState().equals(VmState.RUNNING) ? LbEndpointState.ACTIVE : LbEndpointState.INACTIVE));
            }
            return endpoints;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.listLoadBalancers");
        try {
            Map<String,LoadBalancer> matches = new HashMap<String,LoadBalancer>();
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(LIST_LOAD_BALANCER_RULES);

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
                    doc = method.get(LIST_LOAD_BALANCER_RULES, new Param("pagesize", "500"), new Param("page", nextPage));
                }
                NodeList rules = doc.getElementsByTagName("loadbalancerrule");

                for( int i=0; i<rules.getLength(); i++ ) {
                    Node node = rules.item(i);

                    toRule(node, matches);
                }
            }
            final List<LoadBalancer> results = new ArrayList<LoadBalancer>();
            for( LoadBalancer lb : matches.values() ) {
                if( matchesRegion(lb.getProviderLoadBalancerId()) ) {
                    results.add(lb);
                }
            }
            return results;
        }
        catch( CloudException e ) {
            if( e.getHttpCode() == HttpServletResponse.SC_NOT_FOUND ) {
                return Collections.emptyList();
            }
            throw e;
        }
        finally {
            APITrace.end();
        }
    }

    boolean matchesRegion(@Nonnull String addressId) throws InternalException, CloudException {
        // TODO: add trace
        CSMethod method = new CSMethod(getProvider());
        Document doc = method.get("listPublicIpAddresses", new Param(isId(addressId) ? "ipAddressId" : "ipAddress", addressId));

        NodeList matches = doc.getElementsByTagName("publicipaddress");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node n = matches.item(i);
            NodeList attributes = n.getChildNodes();
            
            for( int j=0; j<attributes.getLength(); j++ ) {
                Node child = attributes.item(j);
                String name = child.getNodeName().toLowerCase();
                String value;
                
                if( child.getChildNodes().getLength() > 0 ) {
                    value = child.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( name.equalsIgnoreCase("zoneid") ) {
                    return (value != null && value.equalsIgnoreCase(getContext().getRegionId()));
                }
            }
        }
        return false;
    }
    
    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public void remove(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        removeLoadBalancer(loadBalancerId);
    }

    @Override
    public void removeLoadBalancer(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.remove");
        try {
            LoadBalancer lb = getLoadBalancer(loadBalancerId);

            if( lb == null || lb.getListeners().length < 1 ) {
                return;
            }
            for( LbListener listener : lb.getListeners() ) {
                String ruleId = getVmOpsRuleId(listener.getAlgorithm(), lb.getAddress(), listener.getPublicPort(), listener.getPrivatePort(), lb.getProviderVlanId());

                if( ruleId != null ) {
                    removeVmOpsRule(ruleId);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeServers(@Nonnull String toLoadBalancerId, @Nonnull String ... serverIds) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "LB.removeServers");
        try {
            LoadBalancer lb = getLoadBalancer(toLoadBalancerId);

            if( lb == null ) {
                throw new CloudException("No such load balancer: " + toLoadBalancerId);
            }
            StringBuilder ids = new StringBuilder();

            for( int i=0; i<serverIds.length; i++ ) {
                ids.append(serverIds[i]);
                if( i < serverIds.length-1 ) {
                    ids.append(",");
                }
            }
            for( LbListener listener : lb.getListeners() ) {
                String ruleId = getVmOpsRuleId(listener.getAlgorithm(), toLoadBalancerId, listener.getPublicPort(), listener.getPrivatePort(), lb.getProviderVlanId());

                Document doc = new CSMethod(getProvider()).get(
                        REMOVE_FROM_LOAD_BALANCER_RULE,
                        new Param("id", ruleId),
                        new Param("virtualMachineIds", ids.toString())
                );

                getProvider().waitForJob(doc, "Remove Server");
            }
        }
        catch( RuntimeException e ) {
            throw new InternalException(e);
        }
        catch( Error e ) {
            throw new InternalException(e);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public SSLCertificate createSSLCertificate( @Nonnull SSLCertificateCreateOptions options ) throws CloudException, InternalException {
        final Document doc = uploadSslCertificate(options, getProvider().getVersionString().startsWith("4.4"));
        String certId = null;
        String certBody = null;
        String certChain = null;
        NodeList matches = doc.getElementsByTagName("sslcert");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node n = matches.item(i);
            NodeList attributes = n.getChildNodes();

            for( int j=0; j<attributes.getLength(); j++ ) {
                Node child = attributes.item(j);
                String name = child.getNodeName().toLowerCase();
                String value;

                if( child.getChildNodes().getLength() > 0 ) {
                    value = child.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( "id".equalsIgnoreCase(name) ) {
                    certId = value;
                }
                else if( "certificate".equalsIgnoreCase(name) ) {
                    certBody = value;
                }
                else if( "certchain".equalsIgnoreCase(name) ) {
                    certChain = value;
                }
            }
        }
        return SSLCertificate.getInstance(certId, certId, null, certBody, certChain, "");
    }

    /**
     * Upload SSL certificate, optionally using parameter double encoding to address CLOUDSTACK-6864 found in 4.4
     * @param opts
     * @param cs44hack
     * @return Document
     */
    private Document uploadSslCertificate(SSLCertificateCreateOptions opts, boolean cs44hack) throws InternalException, CloudException {
        // TODO: add trace
        final List<Param> params = new ArrayList<Param>();
        try {
            params.add(new Param("certificate",
                    cs44hack ? URLEncoder.encode(opts.getCertificateBody(), "UTF-8") : opts.getCertificateBody()));
            params.add(new Param("privatekey",
                    cs44hack ? URLEncoder.encode(opts.getPrivateKey(), "UTF-8") : opts.getPrivateKey()));
            if( opts.getCertificateChain() != null ) {
                params.add(new Param("certchain",
                        cs44hack ? URLEncoder.encode(opts.getCertificateChain(), "UTF-8") : opts.getCertificateChain()));
            }
        } catch (UnsupportedEncodingException e) {
            throw new InternalException(e);
        }
        return new CSMethod(getProvider()).get(UPLOAD_SSL_CERTIFICATE, params);
    }

    @Override
    public @Nonnull Iterable<SSLCertificate> listSSLCertificates() throws CloudException, InternalException {
        // TODO: add trace
        final Document doc = new CSMethod(getProvider()).get(LIST_SSL_CERTIFICATES, new Param("accountid", getProvider().getAccountId()));
        final List<SSLCertificate> results = new ArrayList<SSLCertificate>();

        NodeList matches = doc.getElementsByTagName("sslcert");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node n = matches.item(i);
            NodeList attributes = n.getChildNodes();
            String certId = null;
            String certBody = null;
            String certChain = null;

            for( int j=0; j<attributes.getLength(); j++ ) {
                Node child = attributes.item(j);
                String name = child.getNodeName().toLowerCase();
                String value;

                if( child.getChildNodes().getLength() > 0 ) {
                    value = child.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( "id".equalsIgnoreCase(name) ) {
                    certId = value;
                }
                else if( "certificate".equalsIgnoreCase(name) ) {
                    certBody = value;
                }
                else if( "certchain".equalsIgnoreCase(name) ) {
                    certChain = value;
                }
            }
            results.add(SSLCertificate.getInstance(certId, certId, null, certBody, certChain, ""));
        }

        return results;
    }

    @Override
    public @Nullable SSLCertificate getSSLCertificate( @Nonnull String certificateName ) throws CloudException, InternalException {
        // TODO: add trace
        Document doc = null;
        try {
            doc = new CSMethod(getProvider()).get(LIST_SSL_CERTIFICATES, new Param("certid", certificateName));
        } catch (CSException e) {
            if( e.getHttpCode() == 431 ) {
                return null; // not found
            }
            throw e;
        }
        NodeList matches = doc.getElementsByTagName("sslcert");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node n = matches.item(i);
            NodeList attributes = n.getChildNodes();
            String certId = null;
            String certBody = null;
            String certChain = null;

            for( int j=0; j<attributes.getLength(); j++ ) {
                Node child = attributes.item(j);
                String name = child.getNodeName().toLowerCase();
                String value;

                if( child.getChildNodes().getLength() > 0 ) {
                    value = child.getFirstChild().getNodeValue();
                }
                else {
                    value = null;
                }
                if( "id".equalsIgnoreCase(name) ) {
                    certId = value;
                }
                else if( "certificate".equalsIgnoreCase(name) ) {
                    certBody = value;
                }
                else if( "certchain".equalsIgnoreCase(name) ) {
                    certChain = value;
                }
            }
            return SSLCertificate.getInstance(certId, certId, null, certBody, certChain, "");
        }

        return null;
    }

    @Override
    public void removeSSLCertificate( @Nonnull String certificateName ) throws CloudException, InternalException {
        // TODO: add trace
        Document doc = new CSMethod(getProvider()).get(DELETE_SSL_CERTIFICATE, new Param("id", certificateName));
        NodeList matches = doc.getElementsByTagName("success");
        if( matches.getLength() > 0 ) {
            boolean success = CSCloud.getBooleanValue(matches.item(0));
            if(!success) {
                matches = doc.getElementsByTagName("displaytext");
                if( matches.getLength() > 0 ) {
                    String description = CSCloud.getTextValue(matches.item(0));
                    throw new CloudException("Unable to remove SSL certificate: " + description);
                }
            }
        }

    }

    private void removeVmOpsRule(@Nonnull String ruleId) throws CloudException, InternalException {
        // TODO: add trace
        Document doc = new CSMethod(getProvider()).get(DELETE_LOAD_BALANCER_RULE, new Param("id", ruleId));
        getProvider().waitForJob(doc, "Remove Load Balancer Rule");
    }
    
    private void toRule(@Nullable Node node, @Nonnull Map<String,LoadBalancer> current) throws InternalException, CloudException {
        NodeList attributes = node.getChildNodes();
        int publicPort = -1, privatePort = -1;
        LbAlgorithm algorithm = null;
        String publicIp = null;
        String vlanId = null;
        String ruleId = null;
        String lbName = null;
        String lbDesc = ""; // can't be null
        
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
            if( name.equals("publicip") ) {
                publicIp = value;
            }
            else if( name.equals("networkid") ) {
                vlanId = value;
            }
            else if( name.equals("id") ) {
                ruleId = value;
            }
            else if( name.equals("publicport") && value != null ) {
                publicPort = Integer.parseInt(value);
            }
            else if( name.equals("privateport") && value != null ) {
                privatePort = Integer.parseInt(value);
            }
            else if( name.equals("algorithm") ) {
                if( value == null || value.equals("roundrobin") ) {
                    algorithm = LbAlgorithm.ROUND_ROBIN;
                }
                else if( value.equals("leastconn") ) {
                    algorithm = LbAlgorithm.LEAST_CONN;
                }
                else if( value.equals("") ) {
                    algorithm = LbAlgorithm.SOURCE;
                }
                else {
                    algorithm = LbAlgorithm.ROUND_ROBIN;            
                }
            }
            else if (name.equals("name")) {
                lbName = value;
            }
            else if (name.equals("description")) {
                lbDesc = value;
            }
        }
        LbListener listener = LbListener.getInstance(algorithm, LbPersistence.NONE, LbProtocol.RAW_TCP, publicPort, privatePort);
        Collection<String> serverIds = getServersAt(ruleId);

        if( current.containsKey(publicIp) ) {
            LoadBalancer lb = current.get(publicIp);

            @SuppressWarnings("deprecation") String[] currentIds = lb.getProviderServerIds();
            LbListener[] listeners = lb.getListeners();

            boolean there = false;
            for( LbListener l : listeners ) {
                if( l.getAlgorithm().equals(listener.getAlgorithm()) ) {
                    if( l.getNetworkProtocol().equals(listener.getNetworkProtocol()) ) {
                        if( l.getPublicPort() == listener.getPublicPort() ) {
                            if( l.getPrivatePort() == listener.getPrivatePort() ) {
                                there = true;
                                break;
                            }
                        }
                    }
                }
            }
            if( !there ) {
                lb.withListeners(listener);
            }
            // TODO: WTF?
            TreeSet<String> newIds = new TreeSet<String>();

            Collections.addAll(newIds, currentIds);
            for( String id : serverIds ) {
                newIds.add(id);
            }
            //noinspection deprecation
            lb.setProviderServerIds(newIds.toArray(new String[newIds.size()]));
            //noinspection deprecation
            lb.setName(lbName);
            //noinspection deprecation
            lb.setDescription(lbDesc);
        }
        else {
            Iterable<DataCenter> dcs = getProvider().getDataCenterServices().listDataCenters(getProvider().getContext().getRegionId());
            List<String> ids = new ArrayList<String>();
            for( DataCenter dc : dcs ) {
                ids.add(dc.getProviderDataCenterId());
            }

            LoadBalancer lb = LoadBalancer.getInstance(getContext().getAccountNumber(), getContext().getRegionId(), publicIp, LoadBalancerState.ACTIVE, lbName, lbDesc, LoadBalancerAddressType.IP, publicIp, publicPort).withListeners(listener).operatingIn(ids.toArray(new String[ids.size()]));
            lb.forVlan(vlanId);
            //noinspection deprecation
            lb.setProviderServerIds(serverIds.toArray(new String[serverIds.size()]));
            current.put(publicIp, lb);
        }
    }

    /*
    public @Nullable String getLoadBalancerForAddress(@Nonnull String address) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "LB.getLoadBalancerForAddress");
        try {
            boolean isId = isId(address);
            String key = (isId ? "publicIpId" : "publicIp");
            CSMethod method = new CSMethod(getProvider());

            Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULES, new Param[] { new Param(key, address) }), LIST_LOAD_BALANCER_RULES);
            NodeList rules = doc.getElementsByTagName("loadbalancerrule");

            if( rules.getLength() > 0 ) {
                return address;
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }
    */

    private @Nullable String getRuleId(@Nonnull String loadBalancerId) throws CloudException, InternalException {
        // TODO: add trace
        try {
            final boolean isId = isId(loadBalancerId);
            final String key = (isId ? "publicIpId" : "publicIp");

            final Document doc = new CSMethod(getProvider()).get(LIST_LOAD_BALANCER_RULES, new Param(key, loadBalancerId));
            NodeList rules = doc.getElementsByTagName("loadbalancerrule");

            for( int i=0; i<rules.getLength(); i++ ) {
                NodeList attributes = rules.item(i).getChildNodes();
                String ruleId = null;
                String publicIp = null;
                for( int j=0; j<attributes.getLength(); j++ ) {
                    Node n = attributes.item(j);
                    String name = n.getNodeName().toLowerCase();
                    String value;

                    if( n.getChildNodes().getLength() > 0 ) {
                        value = n.getFirstChild().getNodeValue();
                    }
                    else {
                        value = null;
                    }
                    if( name.equals("publicip") ) {
                        publicIp = value;
                    }
                    else if( name.equals("id") ) {
                        ruleId = value;
                    }
                }
                if( loadBalancerId.equals(publicIp) ) {
                    return ruleId;
                }
            }
        }
        catch( CSException e ) {
            if( e.getHttpCode() == 431 ) {
                return null;
            }
            throw e;
        }
        return null;
    }

    @Override
    public void setTags(@Nonnull String loadBalancerId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	setTags(new String[] { loadBalancerId }, tags);
    }
    
    @Override
    public void setTags(@Nonnull String[] loadBalancerIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "LB.setTags");
    	try {
    		removeTags(loadBalancerIds);
    		getProvider().createTags(loadBalancerIds, "LoadBalancer", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void updateTags(@Nonnull String loadBalancerId, @Nonnull Tag... tags) throws CloudException, InternalException {
    	updateTags(new String[] { loadBalancerId }, tags);
    }
    
    @Override
    public void updateTags(@Nonnull String[] loadBalancerIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "LB.updateTags");
    	try {
    		getProvider().updateTags(loadBalancerIds, "LoadBalancer", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
    
    @Override
    public void removeTags(@Nonnull String loadBalancerId, @Nonnull Tag... tags)
    		throws CloudException, InternalException {
    	removeTags(new String[] { loadBalancerId }, tags);
    }
    
    @Override
    public void removeTags(@Nonnull String[] loadBalancerIds, @Nonnull Tag... tags) throws CloudException, InternalException {
    	APITrace.begin(getProvider(), "LB.removeTags");
    	try {
    		getProvider().removeTags(loadBalancerIds, "LoadBalancer", tags);
    	}
    	finally {
    		APITrace.end();
    	}
    }
}
