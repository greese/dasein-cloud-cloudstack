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

package org.dasein.cloud.cloudstack;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ContextRequirements;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.cloudstack.compute.CSComputeServices;
import org.dasein.cloud.cloudstack.identity.CSIdentityServices;
import org.dasein.cloud.cloudstack.network.CSNetworkServices;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.StorageServices;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class CSCloud extends AbstractCloud {
    static private final Logger logger = getLogger(CSCloud.class, "std");
    static public final String LIST_ACCOUNTS = "listAccounts";
    static private final String LIST_HYPERVISORS = "listHypervisors";
    static private final String LIST_TAGS = "listTags";
    static private final String CREATE_TAGS = "createTags";
    static private final String DELETE_TAGS = "deleteTags";

    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, @Nonnull String type) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("cloudstack") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.cloudstack." + type + "." + pkg + getLastItem(cls.getName()));
    }
    
    public CSCloud() { }
    
    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return "CloudStack";
        }
        String name = ctx.getCloudName();
        
        if( name == null ) {
            return "CloudStack";
        }
        return name;
    }

    @Override
    public @Nonnull ContextRequirements getContextRequirements() {
        return new ContextRequirements(
                new ContextRequirements.Field("apiKey", "The API Keypair", ContextRequirements.FieldType.KEYPAIR, ContextRequirements.Field.ACCESS_KEYS, true)
        );
    }
    
    @Override
    public @Nonnull CSComputeServices getComputeServices() {
        return new CSComputeServices(this);
    }
    
    @Override
    public @Nonnull CSTopology getDataCenterServices() {
        return new CSTopology(this);
    }
    
    @Override
    public @Nullable CSIdentityServices getIdentityServices() {
        if( getVersion().greaterThan(CSVersion.CS21) ) {
            return new CSIdentityServices(this);
        }
        return null;
    }

    @Override
    public @Nonnull CSNetworkServices getNetworkServices() {
        return new CSNetworkServices(this);
    }
    
    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            return "Citrix";
        }
        String name = ctx.getProviderName();
        
        if( name == null ) {
            return "Citrix";
        }
        return name;
    }

    private transient CSServiceProvider serviceProvider;

    public CSServiceProvider getServiceProvider() {
        if( serviceProvider == null ) {
            String pn = getProviderName();

            if( "kt".equalsIgnoreCase(pn) ) {
                serviceProvider = CSServiceProvider.KT;
            }
            else if( "datapipe".equalsIgnoreCase(pn) ) {
                serviceProvider = CSServiceProvider.DATAPIPE;
            }
            else if( "tata".equalsIgnoreCase(pn) ) {
                serviceProvider = CSServiceProvider.TATA;
            } else if( "democloud".equalsIgnoreCase(pn) ) {
                serviceProvider = CSServiceProvider.DEMOCLOUD;
            }
            else {
                serviceProvider = CSServiceProvider.INTERNAL;
            }
        }
        return serviceProvider;
    }

    private transient CSVersion version;

    public @Nonnull
    CSVersion getVersion() {
        if( version == null ) {
            ProviderContext ctx = getContext();
            Properties properties = (ctx == null ? null : ctx.getCustomProperties());
            String versionString;

            if (properties == null || properties.getProperty("apiVersion") == null || properties.getProperty("apiVersion").equals("")) {
               //run list zone query to check whether this might be v4
                try {
                    CSMethod method = new CSMethod(this);
                    String url = method.buildUrl("listZones", new Param("available", "true"));
                    Document doc = method.get(url, "listZones");
                    NodeList meta = doc.getElementsByTagName("listzonesresponse");
                    for (int item = 0; item<meta.getLength(); item++) {
                        Node node = meta.item(item);
                        String v = node.getAttributes().getNamedItem("cloud-stack-version").getNodeValue();
                        if (v.startsWith("4")) {
                            if (properties == null) {
                                properties = new Properties();
                            }
                            properties.setProperty("apiVersion", "CS4");
                            logger.info("Version property not found so setting based on result of query: "+v);
                            version = CSVersion.CS4;
                            return version;
                        }
                    }
                }
                catch (Throwable ignore) {}
                finally {
                    APITrace.end();
                }
            }

            versionString = (properties == null ? "CS3" : properties.getProperty("apiVersion", "CS3"));
            try {
                version = CSVersion.valueOf(versionString);
            }
            catch( Throwable t ) {
                version = CSVersion.CS3;
            }
        }
        return version;
    }

    private boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(this, "CSCloud.isSubscribed");
        try {
            CSMethod method = new CSMethod(this);

            try {
                Document doc = method.get(method.buildUrl(LIST_ACCOUNTS), LIST_ACCOUNTS);
                NodeList matches = doc.getElementsByTagName("user");

                if( matches.getLength() < 1 ) {
                    return false;
                }
                String ctxKey = null;
                try {
                    List<ContextRequirements.Field> fields = getContextRequirements().getConfigurableValues();
                    for(ContextRequirements.Field f : fields ) {
                        if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                            byte[][] keyPair = (byte[][])getContext().getConfigurationValue(f);
                            ctxKey = new String(keyPair[0], "utf-8");
                        }
                    }
                }
                catch( UnsupportedEncodingException e ) {
                    e.printStackTrace();
                    throw new RuntimeException("This cannot happen: " + e.getMessage(), e);
                }

                for( int i=0; i<matches.getLength(); i++ ) {
                    boolean found = false;
                    String account = null;
                    Node node = matches.item(i);
                    NodeList attributes = node.getChildNodes();

                    for (int j = 0; j<attributes.getLength(); j++) {
                        Node attribute = attributes.item(j);
                        String name = attribute.getNodeName().toLowerCase();
                        String value;

                        if( attribute.getChildNodes().getLength() > 0 ) {
                            value = attribute.getFirstChild().getNodeValue();
                        }
                        else {
                            value = null;
                        }
                        if (name.equals("apikey")) {
                            if (value.equals(ctxKey)) {
                                found = true;
                                continue;
                            }
                        }
                        // i think we should match with username, not account name since
                        // the keys belong to user, not the account
                        else if (name.equals("account")) {
                            account = value;
                        }
                    }
                    if (found) {
                        // not sure we need to match this
                        if (!getContext().getAccountNumber().equals(account)) {
                            getContext().setAccountNumber(account);
                        }
                        return true;
                    }
                }
                logger.debug("No match to api key found");
                return false;
            }
            catch( CSException e ) {
                int code = e.getHttpCode();

                if( code == HttpServletResponse.SC_FORBIDDEN || code == 401 || code == 531 ) {
                    return false;
                }
                throw e;
            }
            catch( CloudException e ) {
                int code = e.getHttpCode();

                if( code == HttpServletResponse.SC_FORBIDDEN || code == HttpServletResponse.SC_UNAUTHORIZED ) {
                    return false;
                }
                throw e;
            }
        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnegative long parseTime(@Nonnull String timestamp) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); //2009-02-03T05:26:32.612278
        
        try {
            return df.parse(timestamp).getTime();
        }
        catch( ParseException e ) {
            df = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy"); //Sun Jul 04 02:18:02 EST 2010
            
            try {
                return df.parse(timestamp).getTime();
            }
            catch( ParseException another ) {
                return 0L;
            }
        }        
    }
    
    @Override
    public @Nullable String testContext() {
        APITrace.begin(this, "testContext");
        try {
            try {
                ProviderContext ctx = getContext();

                if( ctx == null ) {
                    return null;
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("testContext(): Checking CSCloud compute credentials");
                }
                if( !isSubscribed() ) {
                    logger.warn("testContext(): CSCloud compute credentials are not subscribed for VM services");
                    return null;
                }
                if( hasStorageServices() ) {
                    if( logger.isDebugEnabled() ) {
                        logger.debug("testContext(): Checking " + ctx.getStorage() + " storage credentials");
                    }
                    StorageServices services = getStorageServices();

                    if( services != null && services.hasOnlineStorageSupport() ) {
                        BlobStoreSupport support = services.getOnlineStorageSupport();

                        if( support != null ) {
                            try {
                                support.list(null).iterator().hasNext();
                            }
                            catch( Throwable t ) {
                                logger.warn("testContext(): Storage credentials failed: " + t.getMessage());
                                if( logger.isDebugEnabled() ) {
                                    logger.debug("textContext(): Storage credentials failed: ", t);
                                }
                                return null;
                            }
                        }
                    }
                }
                if( logger.isInfoEnabled() ) {
                    logger.info("testContext(): Credentials validated");
                }
                return ctx.getAccountNumber();
            }
            catch( Throwable t ) {
                logger.warn("testContext(): Failed to test cloudstack context: " + t.getMessage());
                t.printStackTrace();
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    public Document waitForJob(Document doc, String jobName) throws CloudException, InternalException {
        NodeList matches = doc.getElementsByTagName("jobid");
        if( matches.getLength() > 0 ) {
            return waitForJob(matches.item(0).getFirstChild().getNodeValue(), jobName);
        }    
        return null;
    }
    
    public Document waitForJob(String jobId, String jobName) throws CloudException, InternalException {
        APITrace.begin(this, "waitForJob");
        try {
            CSMethod method = new CSMethod(this);
            String url = method.buildUrl("queryAsyncJobResult", new Param("jobId", jobId));

            while( true ) {
                try { Thread.sleep(5000L); }
                catch( InterruptedException e ) { /* ignore */ }
                Document doc = method.get(url, "queryAsyncJobResult");

                NodeList matches = doc.getElementsByTagName("jobstatus");
                int status = 0;

                if( matches.getLength() > 0 ) {
                    status = Integer.parseInt(matches.item(0).getFirstChild().getNodeValue());
                }
                if( status > 0 ) {
                    int code = status;

                    if( status == 1 ) {
                        return doc;
                    }
                    if( status == 2 ) {
                        matches = doc.getElementsByTagName("jobresult");
                        if( matches.getLength() > 0 ) {
                            String str = matches.item(0).getFirstChild().getNodeValue();

                            if( str == null || str.trim().length() < 1 ) {
                                NodeList nodes = matches.item(0).getChildNodes();
                                String message = null;

                                for( int i=0; i<nodes.getLength(); i++ ) {
                                    Node n = nodes.item(i);

                                    if( n.getNodeName().equalsIgnoreCase("errorcode") ) {
                                        try {
                                            code = Integer.parseInt(n.getFirstChild().getNodeValue().trim());
                                        }
                                        catch( NumberFormatException ignore ) {
                                            // ignore
                                        }
                                    }
                                    else if( n.getNodeName().equalsIgnoreCase("errortext") ) {
                                        message = n.getFirstChild().getNodeValue().trim();
                                    }
                                }
                                CSMethod.ParsedError error = new CSMethod.ParsedError();

                                error.code = code;
                                error.message = message;
                                throw new CSException(error);
                            }
                            else {
                                throw new CloudException(str);
                            }
                        }
                        else {
                            throw new CloudException(jobName + " failed with an unexplained error.");
                        }
                    }
                }
            }
        }
        finally {
            APITrace.end();
        }
    }


    public String getParentAccount() throws CloudException, InternalException {
        return getUserAccountData().getParentAccount();
    }

    public String getDomainId() throws CloudException, InternalException {
        return getUserAccountData().getDomainId();
    }

    public String getAccountId() throws CloudException, InternalException {
        return getUserAccountData().getAccountId();
    }

    public boolean isAdminAccount() throws CloudException, InternalException {
        return getUserAccountData().isAdmin();
    }

    private @Nonnull AccountData getUserAccountData() throws CloudException, InternalException {
        AccountData data = null;
        Cache<AccountData> cache = Cache.getInstance(this, "account", AccountData.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        Iterable<AccountData> cachedValues = cache.get(getContext());
        if( cachedValues != null && cachedValues.iterator().hasNext()) {
            data = cachedValues.iterator().next();
        }
        if( data != null ) {
            return data;
        }
        APITrace.begin(this, "getUserAccountData");

        try {
            CSMethod method = new CSMethod(this);
            String url = method.buildUrl("listAccounts");

            Document doc = method.get(url, "listAccounts");
            NodeList matches = doc.getElementsByTagName("user");

            String ctxKey = null;
            List<ContextRequirements.Field> fields = getContextRequirements().getConfigurableValues();
            for(ContextRequirements.Field f : fields ) {
                if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                    byte[][] keyPair = (byte[][])getContext().getConfigurationValue(f);
                    ctxKey = new String(keyPair[0], "utf-8");
                }
            }


            for (int i = 0; i<matches.getLength(); i++) {
                boolean userAccountFound = false;
                String accountName = null;
                String domainId = null;
                String accountId = null;
                String username = null;
                int accountType = 0;

                NodeList attributes = matches.item(i).getChildNodes();

                for( int j=0; j<attributes.getLength(); j++ ) {
                    Node attribute = attributes.item(j);
                    String name = attribute.getNodeName().toLowerCase();
                    String value;

                    if( attribute.hasChildNodes() && attribute.getChildNodes().getLength() > 0 ) {
                        value = attribute.getFirstChild().getNodeValue();
                    }
                    else {
                        value = null;
                    }

                    if( name.equalsIgnoreCase("username") ) {
                        username = value;
                    }
                    else if (name.equalsIgnoreCase("apikey") && ctxKey.equals(value)) {
                        userAccountFound = true; // user record matched by the context api key
                    }
                    else if (name.equalsIgnoreCase("account")) {
                        accountName = value;
                    }
                    else if (name.equalsIgnoreCase("domainid")) {
                        domainId = value;
                    }
                    else if( "accountid".equalsIgnoreCase(name) ) {
                        accountId = value;
                    }
                    else if( "accounttype".equalsIgnoreCase(name) ) {
                        accountType = Integer.parseInt(value); // 0-user, 1-domain admin, 2-root admin
                    }
                }
                if (userAccountFound) {
                    data = new AccountData(username, accountId, accountName, domainId, accountType > 0);
                    break;
                }
            }
        }
        catch( UnsupportedEncodingException e ) {
            throw new RuntimeException("This cannot happen: " + e.getMessage(), e);
        }
        finally {
            APITrace.end();
        }
        if( data != null ) {
            cache.put(getContext(), Arrays.asList(data));
        }
        else {
            throw new InternalException("Unable to find user account for name " + getContext().getAccountNumber());
        }
        return data;
    }

    class AccountData {
        private String accountId;
        private String parentAccount;
        private String username;
        private String domainId;
        private boolean admin;

        public AccountData( String username, String accountId, String parentAccount, String domainId, boolean admin ) {
            this.username = username;
            this.accountId = accountId;
            this.parentAccount = parentAccount;
            this.domainId = domainId;
            this.admin = admin;
        }

        public String getAccountId() {
            return accountId;
        }

        public String getParentAccount() {
            return parentAccount;
        }

        public String getDomainId() {
            return domainId;
        }

        public boolean isAdmin() {
            return admin;
        }

        public String getUsername() {
            return username;
        }
    }

    /**
     * Returns the text from the given node.
     *
     * @param node the node to extract the value from
     * @return the text from the node
     */
    static public String getTextValue( Node node ) {
        if( node.getChildNodes().getLength() == 0 ) {
            return null;
        }
        return node.getFirstChild().getNodeValue();
    }

    /**
     * Returns the boolean value of the given node.
     *
     * @param node the node to extract the value from
     * @return the boolean value of the node
     */
    static public boolean getBooleanValue( Node node ) {
        return Boolean.valueOf(getTextValue(node));
    }

    public @Nonnull List<String> getZoneHypervisors(String regionId) throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String cacheName = "hypervisorCache";
        Cache<String> hypervisorCache = Cache.getInstance(this, cacheName, String.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));

        List<String> zoneHypervisors = Iterables.toList(hypervisorCache.get(ctx));
        if( zoneHypervisors != null ) {
            return zoneHypervisors;
        }
        try {
            CSMethod method = new CSMethod(this);
            Document doc = method.get(method.buildUrl(LIST_HYPERVISORS, new Param("zoneid", ctx.getRegionId())), LIST_HYPERVISORS);
            NodeList nodes = doc.getElementsByTagName("name");
            zoneHypervisors = new ArrayList<String>();
            for( int i=0; i< nodes.getLength(); i++ ) {
                Node item = nodes.item(i);
                zoneHypervisors.add(item.getFirstChild().getNodeValue().trim());
            }
            hypervisorCache.put(ctx, zoneHypervisors);
            return zoneHypervisors;
        } finally {
        }
    }
    
    public @Nullable void createTags(@Nonnull String[] resIds, @Nonnull String resourceType, Tag... keyValuePairs) throws InternalException, CloudException {
    	APITrace.begin(this, "Cloud.createTags");
    	try {
    		CSMethod method = new CSMethod(this);
    		try {
    			String resourceIds = "";
    			for (String resId : resIds) {
    				resourceIds += resId + ",";
    			}
    			if (resourceIds.endsWith(",")) {
    				resourceIds = resourceIds.substring(0, resourceIds.length() - 1);
    			}
    			List<Param> params = new ArrayList<Param>();
    			params.add(new Param("resourceids", resourceIds));
    			params.add(new Param("resourcetype", resourceType));
    			for (int i = 0; i < keyValuePairs.length; i++) {
    				// Tag value can't be null or ""
    				if (keyValuePairs[i].getValue() != null && !keyValuePairs[i].getValue().equals("")) {
    					params.add(new Param("tags[" + i + "].key", keyValuePairs[i].getKey()));
    					params.add(new Param("tags[" + i + "].value", keyValuePairs[i].getValue()));
    				}
    			}
    			Document doc = method.get(method.buildUrl(CREATE_TAGS, params), CREATE_TAGS);
    			waitForJob(doc, "Create Tags");
    		} catch (CloudException e) {
    			logger.error("Error while creating tags for " + resourceType + " - ", e);
    		}
    	}
    	finally {
    		APITrace.end();
    	}
    }

    public @Nullable void updateTags(@Nonnull String[] resIds, String resourceType, Tag... keyValuePairs) throws InternalException, CloudException {
    	APITrace.begin(this, "Cloud.updateTags");
    	try {
    		try {
    			// List and remove existing tags to update the values
    			for (String resId : resIds) {
    				Tag[] tagList = getTags(resId);
    				if (tagList.length > 0) {
    					List<Tag> tags = new ArrayList<Tag>();
    					// New tags to update
    					for (int i = 0; i < keyValuePairs.length; i++) {
    						// Existing tags
    						for (int k = 0; k < tagList.length; k++) {
    							if (keyValuePairs[i].getKey().equals(tagList[k].getKey())) {
    								if (tagList[k].getValue() != null) {
    									tags.add(new Tag(tagList[k].getKey(), tagList[k].getValue()));
    								}
    							}
    						}
    					}
    					if (tags.size() > 0) {
    						removeTags(new String[] { resId }, resourceType, tags.toArray(new Tag[tags.size()]));
    					}
    				}
    			}
    			// Update tags
    			createTags(resIds, resourceType, keyValuePairs);
    		} catch (CloudException e) {
    			logger.error("Error while updating tags for " + resourceType + " - ", e);
    		}
    	}
    	finally {
    		APITrace.end();
    	}
    }

    public @Nullable void removeTags(@Nonnull String[] vmIds, String resourceType, Tag... keyValuePairs) throws InternalException, CloudException {
    	APITrace.begin(this, "Cloud.removeTags");
    	try {
    		CSMethod method = new CSMethod(this);
    		String resourceIds = "";
    		try {
    			for (String vmId : vmIds) {
    				resourceIds += vmId + ",";
    			}
    			if (resourceIds.endsWith(",")) {
    				resourceIds = resourceIds.substring(0, resourceIds.length() - 1);
    			}
    			List<Param> params = new ArrayList<Param>();
    			params.add(new Param("resourceids", resourceIds));
    			params.add(new Param("resourcetype", resourceType));
    			for (int i = 0; i < keyValuePairs.length; i++) {
    				// Tag value can't be null or ""
    				if (keyValuePairs[i].getValue() != null && !keyValuePairs[i].getValue().equals("")) {
    					params.add(new Param("tags[" + i + "].key", keyValuePairs[i].getKey()));
    					params.add(new Param("tags[" + i + "].value", keyValuePairs[i].getValue()));
    				}
    			}
    			Document doc = method.get(method.buildUrl(DELETE_TAGS, params), DELETE_TAGS);
    			waitForJob(doc, "Delete Tags");
    		} catch (CloudException e) {
    			logger.error("Error while removing tags for " + resourceType + " - ", e);
    		}
    	}
    	finally {
    		APITrace.end();
    	}
    }

    public @Nullable Tag[] getTags(@Nonnull String resourceId) throws InternalException, CloudException {
    	APITrace.begin(this, "Cloud.listTags");
    	try {
    		CSMethod method = new CSMethod(this);
    		List<Tag> tags = new ArrayList<Tag>();
    		try {
    			Document doc = method.get(method.buildUrl(LIST_TAGS, new Param( "resourceid", resourceId)), LIST_TAGS);
    			NodeList matches = doc.getElementsByTagName("tag");
    			if (matches.getLength() > 0) {
    				for (int i = 0; i < matches.getLength(); i++) {
    					// Child 0 has the key, child 1 has the value
    					tags.add(new Tag(matches.item(i).getChildNodes().item(0).getTextContent(), matches.item(i).getChildNodes().item(1).getTextContent()));
    				}
    			}
    		} catch (CloudException e) {
    			logger.error("Error while listing the tags for - " + resourceId + ".", e);
    		}
    		return tags.toArray(new Tag[tags.size()]);
    	}
    	finally {
    		APITrace.end();
    	}
    }
}