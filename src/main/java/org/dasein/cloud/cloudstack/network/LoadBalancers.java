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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSException;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.CSTopology;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.LbAlgorithm;
import org.dasein.cloud.network.LbListener;
import org.dasein.cloud.network.LbProtocol;
import org.dasein.cloud.network.LoadBalancer;
import org.dasein.cloud.network.LoadBalancerAddressType;
import org.dasein.cloud.network.LoadBalancerState;
import org.dasein.cloud.network.LoadBalancerSupport;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class LoadBalancers implements LoadBalancerSupport { 
    static public final String ASSIGN_TO_LOAD_BALANCER_RULE       = "assignToLoadBalancerRule";
    static public final String CREATE_LOAD_BALANCER_RULE          = "createLoadBalancerRule";
    static public final String DELETE_LOAD_BALANCER_RULE          = "deleteLoadBalancerRule";
    static public final String LIST_LOAD_BALANCER_RULES           = "listLoadBalancerRules";
    static public final String LIST_LOAD_BALANCER_RULE_INSTANCES  = "listLoadBalancerRuleInstances";
    static public final String REMOVE_FROM_LOAD_BALANCER_RULE     = "removeFromLoadBalancerRule";
    
    private CSCloud provider;
    
    LoadBalancers(CSCloud provider) {
        this.provider = provider;
    }
    
    @Override
    public void addDataCenters(String toLoadBalancerId, String ... dataCenterIds) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void addServers(String toLoadBalancerId, String ... serverIds) throws CloudException, InternalException {
        try {
            LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
    
            if( lb == null ) {
                throw new CloudException("No such load balancer: " + toLoadBalancerId);
            }
            if( serverIds == null || serverIds.length < 1 ) {
                return;
            }
            for( LbListener listener : lb.getListeners() ) {
                String ruleId = getVmOpsRuleId(listener.getAlgorithm(), toLoadBalancerId, listener.getPublicPort(), listener.getPrivatePort());
                StringBuilder str = new StringBuilder();
                
                for( int i=0; i<serverIds.length; i++ ) {
                    str.append(serverIds[i]);
                    if( i < serverIds.length-1 ) {
                        str.append(",");
                    }
                }
                CSMethod method = new CSMethod(provider);
                Document doc = method.get(method.buildUrl(ASSIGN_TO_LOAD_BALANCER_RULE, new Param("id", ruleId), new Param("virtualMachineIds", str.toString())));
                
                provider.waitForJob(doc, "Add Server");
            }
        }
        catch( RuntimeException e ) {
            throw new InternalException(e);
        }
        catch( Error e ) {
            throw new InternalException(e);
        }
    }

    @Override
    public String create(String name, String description, String addressId, String[] dcIds, LbListener[] listeners, String[] servers) throws CloudException, InternalException {
        try {
        org.dasein.cloud.network.IpAddress publicAddress = provider.getNetworkServices().getIpAddressSupport().getIpAddress(addressId);
        
        if( publicAddress == null ) {
            throw new CloudException("You must specify the IP address for your load balancer.");
        }
        for( LbListener listener : listeners ) {
            if( !isId(addressId) ) {
                createVmOpsRule(listener.getAlgorithm(), addressId, listener.getPublicPort(), listener.getPrivatePort());
            }
            else {
                createCloudstack22Rule(listener.getAlgorithm(), addressId, listener.getPublicPort(), listener.getPrivatePort());
            }
        }
        if( servers != null ) {
            this.addServers(publicAddress.getAddress(), servers);
        }
        return publicAddress.getAddress();
        }
        catch( CloudException e ) {
            e.printStackTrace();
            throw e;
        }
        catch( InternalException e ) {
            e.printStackTrace();
            throw e;
        }
        catch( RuntimeException e ) {
            e.printStackTrace();
            throw e;
        }
    }

    private void createVmOpsRule(LbAlgorithm algorithm, String publicIp, int publicPort, int privatePort) throws CloudException, InternalException {
        String id = getVmOpsRuleId(algorithm, publicIp, publicPort, privatePort);
        
        if( id != null ) {
            return;
        }
        Param[] params = new Param[6];
        String algor;
        
        switch( algorithm ) {
            case ROUND_ROBIN: algor = "roundrobin"; break;
            case LEAST_CONN: algor = "leastconn"; break;
            case SOURCE: algor = "source"; break;
            default: algor = "roundrobin"; break;
        }
        params[0] = new Param("publicIp", publicIp);
        params[1] = new Param("publicPort", String.valueOf(publicPort));
        params[2] = new Param("privatePort", String.valueOf(privatePort));
        params[3] = new Param("algorithm", algor);
        params[4] = new Param("name", "dsnlb_" + publicIp + "_" + publicPort + "_" + privatePort);
        params[5] = new Param("description", "dsnlb_" + publicIp + "_" + publicPort + "_" + privatePort);
        
        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl(CREATE_LOAD_BALANCER_RULE, params));
        NodeList matches = doc.getElementsByTagName("loadbalancerrule"); // v2.1
        
        for( int i=0; i<matches.getLength(); i++ ) {
            HashMap<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
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
            HashMap<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
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
    
    private void createCloudstack22Rule(LbAlgorithm algorithm, String publicIpId, int publicPort, int privatePort) throws CloudException, InternalException {
        String id = getVmOpsRuleId(algorithm, publicIpId, publicPort, privatePort);
        
        if( id != null ) {
            return;
        }
        
        Param[] params = new Param[6];
        String algor;
        
        switch( algorithm ) {
            case ROUND_ROBIN: algor = "roundrobin"; break;
            case LEAST_CONN: algor = "leastconn"; break;
            case SOURCE: algor = "source"; break;
            default: algor = "roundrobin"; break;
        }
        params[0] = new Param("publicIpId", publicIpId);
        params[1] = new Param("publicPort", String.valueOf(publicPort));
        params[2] = new Param("privatePort", String.valueOf(privatePort));
        params[3] = new Param("algorithm", algor);
        params[4] = new Param("name", "dsnlb_" + publicIpId + "_" + publicPort + "_" + privatePort);
        params[5] = new Param("description", "dsnlb_" + publicIpId + "_" + publicPort + "_" + privatePort);

        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl(CREATE_LOAD_BALANCER_RULE, params));
        NodeList matches = doc.getElementsByTagName("loadbalancerrule"); // v2.1
        
        for( int i=0; i<matches.getLength(); i++ ) {
            HashMap<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
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
            HashMap<String,LoadBalancer> current = new HashMap<String,LoadBalancer>();
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
        	provider.waitForJob(doc, "Create Load Balancer Rule");
        	return;
        }
        
        throw new CloudException("Failed to add load balancer rule (2).");
    }
    
    @Override
    public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }
    
    private boolean isId(String ipAddressIdCandidate) {
        String[] parts = ipAddressIdCandidate.split("\\.");
        
        return (parts == null || parts.length != 4 );        
    }
    
    @Override
    public LoadBalancer getLoadBalancer(String loadBalancerId) throws CloudException, InternalException {
        try {
            HashMap<String,LoadBalancer> matches = new HashMap<String,LoadBalancer>();
            boolean isId = isId(loadBalancerId);
            String key = (isId ? "publicIpId" : "publicIp");

            CSMethod method = new CSMethod(provider);
            Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULES, new Param[] { new Param(key, loadBalancerId) }));
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
    
    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 0;
    }

    @Override
    public String getProviderTermForLoadBalancer(Locale locale) {
        return "load balancer";
    }
    
    static private volatile List<LbAlgorithm> algorithms = null;
    
    @Override
    public Iterable<LbAlgorithm> listSupportedAlgorithms() {
        List<LbAlgorithm> list = algorithms;
        
        if( list == null ) {
            list = new ArrayList<LbAlgorithm>();
            list.add(LbAlgorithm.ROUND_ROBIN);
            list.add(LbAlgorithm.LEAST_CONN);
            list.add(LbAlgorithm.SOURCE);
            algorithms = Collections.unmodifiableList(list);
        }
        return algorithms;
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    static private volatile List<LbProtocol> protocols = null;
    
    @Override
    public Iterable<LbProtocol> listSupportedProtocols() {
        List<LbProtocol> list = protocols;
        
        if( protocols == null ) {
            list = new ArrayList<LbProtocol>();
            list.add(LbProtocol.RAW_TCP);
            protocols = Collections.unmodifiableList(list);
        }
        return protocols;
    }
    
    private Collection<String> getServersAt(String ruleId) throws InternalException, CloudException {
        ArrayList<String> ids = new ArrayList<String>();
        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULE_INSTANCES, new Param[] { new Param("id", ruleId) }));
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
        return ids;
    }
    
    private String getVmOpsRuleId(LbAlgorithm lbAlgorithm, String publicIp, int publicPort, int privatePort) throws CloudException, InternalException {
        String ruleId = null;
        String algorithm;

        switch( lbAlgorithm ) {
            case ROUND_ROBIN: algorithm = "roundrobin"; break;
            case LEAST_CONN: algorithm = "leastconn"; break;
            case SOURCE: algorithm = "source"; break;
            default: algorithm = "roundrobin"; break;
        }
        boolean isId = isId(publicIp);
        String key = (isId ? "publicIpId" : "publicIp");
        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULES, new Param[] { new Param(key, publicIp) }));
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
                    if( !value.equals(publicIp) ) {
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
                    if( value == null || !value.equals(algorithm) ) {
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
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isDataCenterLimited() {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public boolean requiresListenerOnCreate() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean requiresServerOnCreate() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        CSMethod method = new CSMethod(provider);
        
        try {
            method.get(method.buildUrl(CSTopology.LIST_ZONES, new Param[] { new Param("available", "true") }));
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

    @Override
    public boolean supportsMonitoring() {
        return false;
    }
    
    @Override
    public Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        HashMap<String,LoadBalancer> matches = new HashMap<String,LoadBalancer>();
        CSMethod method = new CSMethod(provider);
        
        try {
            Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULES, new Param[0] ));
            NodeList rules = doc.getElementsByTagName("loadbalancerrule");
            
            for( int i=0; i<rules.getLength(); i++ ) {
                Node node = rules.item(i);
                
                toRule(node, matches);
            }
            ArrayList<LoadBalancer> results = new ArrayList<LoadBalancer>();
            
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
            e.printStackTrace();
            throw e;
        }
    }

    boolean matchesRegion(String addressId) throws InternalException, CloudException {
        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl("listPublicIpAddresses", new Param[] { new Param(isId(addressId) ? "ipAddressId" : "ipAddress", addressId) }));
        HashMap<String,LoadBalancer> loadBalancers = new HashMap<String,LoadBalancer>();
        LoadBalancer lb = provider.getNetworkServices().getLoadBalancerSupport().getLoadBalancer(addressId);
        
        loadBalancers.put(addressId, lb);
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
                    return (value != null && value.equalsIgnoreCase(provider.getContext().getRegionId()));
                }
            }
        }
        return false;
    }
    
    @Override
    public void remove(String loadBalancerId) throws CloudException, InternalException {
        LoadBalancer lb = getLoadBalancer(loadBalancerId);
        
        if( lb.getListeners() == null ) {
            return;
        }
        for( LbListener listener : lb.getListeners() ) {
            String ruleId = getVmOpsRuleId(listener.getAlgorithm(), lb.getAddress(), listener.getPublicPort(), listener.getPrivatePort());
            
            if( ruleId != null ) {
                removeVmOpsRule(ruleId);
            }
        }
    }

    @Override
    public void removeDataCenters(String fromLoadBalancerId, String ... dataCenterIds) throws CloudException, InternalException {
        throw new OperationNotSupportedException("These load balancers are not data center based.");
    }

    @Override
    public void removeServers(String toLoadBalancerId, String ... serverIds) throws CloudException, InternalException {
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
                String ruleId = getVmOpsRuleId(listener.getAlgorithm(), toLoadBalancerId, listener.getPublicPort(), listener.getPrivatePort());
                CSMethod method = new CSMethod(provider);
                Document doc = method.get(method.buildUrl(REMOVE_FROM_LOAD_BALANCER_RULE, new Param[] { new Param("id", ruleId), new Param("virtualMachineIds", ids.toString()) }));

                provider.waitForJob(doc, "Remove Server");
            }
        }
        catch( RuntimeException e ) {
            throw new InternalException(e);
        }
        catch( Error e ) {
            throw new InternalException(e);
        }
    }

    private void removeVmOpsRule(String ruleId) throws CloudException, InternalException {
        CSMethod method = new CSMethod(provider);
        Document doc = method.get(method.buildUrl(DELETE_LOAD_BALANCER_RULE, new Param[] { new Param("id", ruleId) }));

        provider.waitForJob(doc, "Remove Load Balancer Rule");
    }
    
    private void toRule(Node node, Map<String,LoadBalancer> current) throws InternalException, CloudException {
        NodeList attributes = node.getChildNodes();
        int publicPort = -1, privatePort = -1;
        LbAlgorithm algorithm = null;
        String publicIp = null;
        String ruleId = null;
        
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
                if( algorithm == null || algorithm.equals("roundrobin") ) {
                    algorithm = LbAlgorithm.ROUND_ROBIN;
                }
                else if( algorithm.equals("leastconn") ) {
                    algorithm = LbAlgorithm.LEAST_CONN;
                }
                else if( algorithm.equals("") ) {
                    algorithm = LbAlgorithm.SOURCE;
                }
                else {
                    algorithm = LbAlgorithm.ROUND_ROBIN;            
                }
            }
        }
        LbListener listener = new LbListener();
        
        listener.setAlgorithm(algorithm);
        listener.setNetworkProtocol(LbProtocol.RAW_TCP);
        listener.setPrivatePort(privatePort);
        listener.setPublicPort(publicPort);
        Collection<String> serverIds = getServersAt(ruleId);

        if( current.containsKey(publicIp) ) {
            LoadBalancer lb = current.get(publicIp);
            
            LbListener[] listeners = lb.getListeners();
            String[] currentIds = lb.getProviderServerIds();
            TreeSet<Integer> ports = new TreeSet<Integer>();
            
            for( int port : lb.getPublicPorts() ) {
                ports.add(port);
            }
            ports.add(publicPort);
            
            int[] portList = new int[ports.size()];
            int i = 0;
            
            for( Integer p : ports ) {
                portList[i++] = p.intValue();
            }
            lb.setPublicPorts(portList);
            i = 0;
            boolean there = false;
            
            LbListener[] newList = new LbListener[listeners.length];
            
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
                newList[i++] = l;
            }
            if( !there ) {
                LbListener[] tmp = new LbListener[newList.length + 1];
                
                tmp[i++] = listener;
                lb.setListeners(tmp);
            }
            
            TreeSet<String> newIds = new TreeSet<String>();
            
            for( String id : currentIds ) {
                newIds.add(id);                
            }
            for( String id : serverIds ) {
                newIds.add(id);                
            }
            lb.setProviderServerIds(newIds.toArray(new String[newIds.size()]));
        }
        else {
            LoadBalancer lb = new LoadBalancer();
            
            lb.setCurrentState(LoadBalancerState.ACTIVE);
            lb.setAddress(publicIp);
            lb.setAddressType(LoadBalancerAddressType.IP);
            lb.setDescription(publicIp +":" + publicPort + " -> RAW_TCP:" + privatePort);
            lb.setName(publicIp);
            lb.setProviderOwnerId(provider.getContext().getAccountNumber());
            lb.setCreationTimestamp(0L);
            lb.setPublicPorts(new int[] { publicPort });
            Collection<DataCenter> dcs = provider.getDataCenterServices().listDataCenters(provider.getContext().getRegionId());
            String[] ids = new String[dcs.size()];
            int i =0;
            
            for( DataCenter dc : dcs ) {
                ids[i++] = dc.getProviderDataCenterId();
            }
            lb.setProviderDataCenterIds(ids);
            lb.setProviderLoadBalancerId(publicIp);
            lb.setProviderRegionId(provider.getContext().getRegionId());
            lb.setProviderServerIds(serverIds.toArray(new String[serverIds.size()]));
            lb.setListeners(new LbListener[] { listener });
            current.put(publicIp, lb);
        }
    }

    public String getLoadBalancerForAddress(String address) throws InternalException, CloudException {
        boolean isId = isId(address);
        String key = (isId ? "publicIpId" : "publicIp");
        CSMethod method = new CSMethod(provider);

        Document doc = method.get(method.buildUrl(LIST_LOAD_BALANCER_RULES, new Param[] { new Param(key, address) }));
        NodeList rules = doc.getElementsByTagName("loadbalancerrule");
        
        if( rules.getLength() > 0 ) {
            return address;
        }        
        return null;
    }
}
