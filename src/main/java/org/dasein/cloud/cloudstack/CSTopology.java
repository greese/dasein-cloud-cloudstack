/**
 * Copyright (C) 2009-2013 enstratius, Inc.
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
import java.util.Properties;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterCapabilities;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Folder;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.dc.ResourcePool;
import org.dasein.cloud.dc.StoragePool;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CSTopology implements DataCenterServices {
    static public final String LIST_ZONES = "listZones";
    
    private CSCloud provider;
    
    public CSTopology(@Nonnull CSCloud provider) {
        this.provider = provider;
    }

    private transient volatile CSTopologyCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new CSTopologyCapabilities(provider);
        }
        return capabilities;
    }

    public @Nullable DataCenter getDataCenter(@Nonnull String zoneId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getDataCenter");
        try {
            for( Region region : listRegions() ) {
                for( DataCenter dc : listDataCenters(region.getProviderRegionId()) ) {
                    if( dc.getProviderDataCenterId().equals(zoneId) ) {
                        return dc;
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "zone";
    }

    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    public @Nullable Region getRegion(@Nonnull String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getRegion");
        try {
            for( Region region : listRegions() ) {
                if( region.getProviderRegionId().equals(regionId) ) {
                    return region;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    public boolean requiresNetwork(@Nonnull String zoneId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.requiresNetwork");
        try {
            CSMethod method = new CSMethod(provider);
            String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
            Document doc = method.get(url, LIST_ZONES);

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
        finally {
            APITrace.end();
        }
    }
    
    public boolean supportsSecurityGroups(@Nonnull String zoneId, boolean basicOnly) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.supportsSecurityGroups");
        try {
            CSMethod method = new CSMethod(provider);
            String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
            Document doc = method.get(url, LIST_ZONES);
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
        finally {
            APITrace.end();
        }
    }
    
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            Cache<DataCenter> cache = Cache.getInstance(provider, "dataCenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Collection<DataCenter> dcs = (Collection<DataCenter>)cache.get(ctx);

            if( dcs == null ) {
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
                dcs = Collections.singletonList(zone);
                cache.put(ctx, dcs);
            }
            return dcs;
        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listRegions");
        try {
            Cache<Region> cache = Cache.getInstance(provider, "regions", Region.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            Collection<Region> regions = (Collection<Region>)cache.get(ctx);

            if( regions == null ) {
                CSMethod method = new CSMethod(provider);
                String url = method.buildUrl(LIST_ZONES, new Param("available", "true"));
                Document doc = method.get(url, LIST_ZONES);

                regions = new ArrayList<Region>();
                NodeList matches = doc.getElementsByTagName("zone");
                for( int i=0; i<matches.getLength(); i++ ) {
                    Region r = toRegion(matches.item(i));

                    if( r != null ) {
                        if (provider.getProviderName().contains("Datapipe")) {
                            // don't return Shanghai region as there are Chinese license concerns
                            if (r.getName().contains("Shanghai")) {
                                continue;
                            }
                        }
                        regions.add(r);
                    }
                }
                cache.put(ctx, regions);
            }
            return regions;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
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
        ProviderContext ctx = provider.getContext();

        Properties p = (ctx == null ? null : ctx.getCustomProperties());

        return (p == null ? "US" : p.getProperty("locale." + name, "US"));
    }

    @Nonnull
    @Override
    public StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Nonnull
    @Override
    public Collection<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }
}
