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

package org.dasein.cloud.cloudstack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Zones implements DataCenterServices {
    static public final String LIST_ZONES = "listZones";
    
    private CloudstackProvider provider;
    
    public Zones(@Nonnull CloudstackProvider provider) {
        this.provider = provider;
    }
    
    public @Nullable DataCenter getDataCenter(@Nonnull String zoneId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            for( DataCenter dc : listDataCenters(region.getProviderRegionId()) ) {
                if( dc.getProviderDataCenterId().equals(zoneId) ) {
                    return dc;
                }
            }
        }
        return null;
    }

    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "zone";
    }

    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    public @Nullable Region getRegion(@Nonnull String regionId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            if( region.getProviderRegionId().equals(regionId) ) {
                return region;
            }
        }
        return null;
    }

    public boolean requiresNetwork(@Nonnull String zoneId) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
        Document doc = method.get(url);

        NodeList matches = doc.getElementsByTagName("zone");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node zone = matches.item(i);
            
            if( zone.hasChildNodes() ) {
                NodeList attrs = zone.getChildNodes();
                String networkType = null;
                String id = null;
                
                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);
                    
                    if( attr.getNodeName().equalsIgnoreCase("id") ) {
                        id = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equalsIgnoreCase("networkType") ) {
                        networkType = attr.getFirstChild().getNodeValue().trim();
                    }
                }
                if( zoneId.equals(id) ) {
                    return !("basic".equalsIgnoreCase(networkType));
                }
            }
        }
        return true;
    }
    
    public boolean supportsSecurityGroups(@Nonnull String zoneId, boolean basicOnly) throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
        Document doc = method.get(url);
        boolean sg = false;
        boolean basic = false;
        
        NodeList matches = doc.getElementsByTagName("zone");
        for( int i=0; i<matches.getLength(); i++ ) {
            Node zone = matches.item(i);
            
            if( zone.hasChildNodes() ) {
                NodeList attrs = zone.getChildNodes();
                boolean groups = false;
                String networkType = null;
                String id = null;
                
                for( int j=0; j<attrs.getLength(); j++ ) {
                    Node attr = attrs.item(j);
                    
                    if( attr.getNodeName().equalsIgnoreCase("id") ) {
                        id = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equalsIgnoreCase("networkType") ) {
                        networkType = attr.getFirstChild().getNodeValue().trim();
                    }
                    else if( attr.getNodeName().equalsIgnoreCase("securitygroupsenabled") ) {
                        groups = attr.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true");
                    }
                }
                if( zoneId.equals(id) ) {
                    basic = "basic".equalsIgnoreCase(networkType);
                    sg = groups;
                    break;
                }
            }
        }
        return ((!basicOnly || basic) && sg);
    }
    
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String regionId) throws InternalException, CloudException {
        Region region = getRegion(regionId);
        
        if( region == null ) {
            throw new CloudException("No such region: " + regionId);
        }
        DataCenter zone = new DataCenter();
        
        zone.setActive(true);
        zone.setAvailable(true);
        zone.setName(region.getName() + " (DC)");
        zone.setProviderDataCenterId(regionId);
        zone.setRegionId(regionId);
        return Collections.singletonList(zone);
    }

    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        CloudstackMethod method = new CloudstackMethod(provider);
        String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
        Document doc = method.get(url);

        ArrayList<Region> regions = new ArrayList<Region>();
        NodeList matches = doc.getElementsByTagName("zone");
        for( int i=0; i<matches.getLength(); i++ ) {
            Region r = toRegion(matches.item(i));
            
            if( r != null ) {
                regions.add(r);
            }
        }
        return regions;
    }
    
    private @Nullable Region toRegion(@Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        Region region = new Region();
        
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
            if( name.equals("id") ) {
                region.setProviderRegionId(value);
            }
            else if( name.equals("name") ) {
                region.setName(value);
            }
        }
        if( region.getProviderRegionId() == null ) {
            return null;
        }
        if( region.getName() == null ) {
            region.setName(region.getProviderRegionId());
        }
        region.setActive(true);
        region.setAvailable(true);
        region.setJurisdiction(getJurisdiction(region.getName()));
        return region;
    }
    
    private String getJurisdiction(String name) {
        if( name.contains("New York") ) {
            return "US";
        }
        else if( name.contains("Hong Kong") ) {
            return "HK";
        }
        else if( name.contains("India") ) {
            return "IN";
        }
        else if( name.contains("London") ) {
            return "EU";
        }
        String cc = Locale.getDefault().getCountry();
        
        return (cc == null ? "US" : cc);
    }
            
}
