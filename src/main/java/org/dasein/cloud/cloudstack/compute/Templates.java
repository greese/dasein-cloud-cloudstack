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

package org.dasein.cloud.cloudstack.compute;

import java.util.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.cloudstack.*;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageCapabilities;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Templates extends AbstractImageSupport<CSCloud> {
    static public final Logger logger = Logger.getLogger(Templates.class);

    static private final String CREATE_TEMPLATE             = "createTemplate";
    static private final String DELETE_ISO                  = "deleteIso";
    static private final String DELETE_TEMPLATE             = "deleteTemplate";
    static private final String LIST_ISOS                   = "listIsos";
    static private final String LIST_OS_TYPES               = "listOsTypes";
    static private final String LIST_ISO_PERMISSIONS        = "listIsoPermissions";
    static private final String LIST_TEMPLATE_PERMISSIONS   = "listTemplatePermissions";
    static private final String LIST_TEMPLATES              = "listTemplates";
    static private final String REGISTER_TEMPLATE           = "registerTemplate";
    static private final String UPDATE_ISO_PERMISSIONS      = "updateIsoPermissions";
    static private final String UPDATE_TEMPLATE_PERMISSIONS = "updateTemplatePermissions";
    
    public Templates(CSCloud provider) {
        super(provider);
    }

    @Override
    public void addImageShare( @Nonnull String providerImageId, @Nonnull String accountNumber ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.addImageShare");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            MachineImage img = getImage(providerImageId);

            if( img == null ) {
                return;
            }
            if( !ctx.getAccountNumber().equals(img.getProviderOwnerId())
                    && !getProvider().getParentAccount().equalsIgnoreCase(img.getProviderOwnerId())) {
                return;
            }
            Param[] params = new Param[]{new Param("id", providerImageId), new Param("accounts", accountNumber), new Param("op", "add")};

            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(UPDATE_TEMPLATE_PERMISSIONS, params), UPDATE_TEMPLATE_PERMISSIONS);
                getProvider().waitForJob(doc, "Share Template");
            }
            catch( CSException e ) {
                if( e.getHttpCode() == 431 ) {
                    //try update iso share
                    doc = method.get(method.buildUrl(UPDATE_ISO_PERMISSIONS, params), UPDATE_ISO_PERMISSIONS);
                    getProvider().waitForJob(doc, "Share Iso");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void addPublicShare( @Nonnull String providerImageId ) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.addPublicShare");
        try {
            MachineImage img = getImage(providerImageId);

            if( img == null ) {
                return;
            }
            if( !getContext().getAccountNumber().equals(img.getProviderOwnerId())
                    && !getProvider().getParentAccount().equalsIgnoreCase(img.getProviderOwnerId())) {
                return;
            }
            Param[] params = new Param[]{new Param("id", providerImageId), new Param("isPublic", "true") };

            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(UPDATE_TEMPLATE_PERMISSIONS, params), UPDATE_TEMPLATE_PERMISSIONS);
                getProvider().waitForJob(doc, "Share Template");
            }
            catch (CSException e) {
                if (e.getHttpCode()==431) {
                    //try update iso share
                    doc = method.get(method.buildUrl(UPDATE_ISO_PERMISSIONS, params), UPDATE_ISO_PERMISSIONS);
                    getProvider().waitForJob(doc, "Share Iso");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile CSTemplateCapabilities capabilities;
    @Override
    public ImageCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new CSTemplateCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.getImage");
        try {
            CSMethod method = new CSMethod(getProvider());
            String url = method.buildUrl(LIST_TEMPLATES, new Param("id", providerImageId), new Param("templateFilter", "executable"), new Param("zoneId", getContext().getRegionId()));
            Document doc;
            boolean isTemplate = true;
            try {
                doc = method.get(url, LIST_TEMPLATES);
            }
            catch( CSException e ) {
                if( e.getHttpCode() == 431 ) {
                    //check if we can find a match to iso with this id
                    url = method.buildUrl(LIST_ISOS, new Param("id", providerImageId), new Param("isoFilter", "executable"), new Param("zoneId", getContext().getRegionId()), new Param("bootable", "true"));
                    try {
                        doc = method.get(url, LIST_ISOS);
                        isTemplate = false;
                    }
                    catch( CSException ex ) {
                        if( ex.getHttpCode() == 431 ) {
                            return null;
                        }
                        if( ex.getMessage() != null && (ex.getMessage().contains("specify a valid template ID") || ex.getMessage().contains("does not have permission")) ) {
                            return null;
                        }
                        throw ex;
                    }
                }
                else if( e.getMessage() != null && (e.getMessage().contains("specify a valid template ID") || e.getMessage().contains("does not have permission")) ) {
                    return null;
                }
                else {
                    throw e;
                }
            }
            NodeList matches;
            if (isTemplate) {
                matches = doc.getElementsByTagName("template");
            }
            else {
                matches = doc.getElementsByTagName("iso");
            }
            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);

                MachineImage image = toImage(node, false, null);

                if( image != null ) {
                    if (!isTemplate) {
                        image.setTag("isISO", "true");
                    }
                    return image;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable String getRootVolume(@Nonnull String serverId) throws InternalException, CloudException {
        return getProvider().getComputeServices().getVolumeSupport().getRootVolumeId(serverId);
    }
    
    private Architecture guess(String desc) {
        Architecture arch = Architecture.I64;
        
        if( desc.contains("x64") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("x32") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("64 bit") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("32 bit") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("i386") ) {
            arch = Architecture.I32;
        }
        else if( desc.contains("64") ) {
            arch = Architecture.I64;
        }
        else if( desc.contains("32") ) {
            arch = Architecture.I32;
        }
        return arch;
    }
    
    private void guessSoftware(@Nonnull MachineImage image) {
        String[] components = ((image.getName() + " " + image.getDescription()).toLowerCase()).split(",");
        StringBuilder software = new StringBuilder();
        boolean comma = false;

        if( components == null || components.length < 0 ) {
            components = new String[] { (image.getName() + " " + image.getDescription()).toLowerCase() };
        }
        for( String str : components ) {
            if( str.contains("sql server") ) {
                if( comma ) {
                    software.append(",");
                }
                if( str.contains("sql server 2008") ) {
                    software.append("SQL Server 2008");
                }
                else if( str.contains("sql server 2005") ) {
                    software.append("SQL Server 2005");
                }
                else {
                    software.append("SQL Server 2008");
                }
                comma = true;
            }
        }
        image.withSoftware(software.toString());
    }

    @Override
    protected @Nonnull MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.capture");
        try {
            String vmId = options.getVirtualMachineId();
            if( vmId == null ) {
                throw new OperationNotSupportedException("Only options based off of servers are supported");
            }
            VirtualMachine server = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);

            if( server == null ) {
                throw new CloudException("No such server: " + vmId);
            }
            CSMethod method = new CSMethod(getProvider());
            boolean restart = false;
            if (!server.getCurrentState().equals(VmState.STOPPED)) {
                restart = true;
                getProvider().getComputeServices().getVirtualMachineSupport().stop(vmId);
                long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 10);
                server = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
                while (timeout > System.currentTimeMillis()) {
                    if (server.getCurrentState().equals(VmState.STOPPED)) {
                        break;
                    }
                    server = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
                    try {Thread.sleep(15000l);}
                    catch (InterruptedException ignore) {}
                }
            }
            Document doc;

            String rootVolumeId = getRootVolume(vmId);

            if( rootVolumeId == null ) {
                throw new CloudException("No root volume is attached to the target server.");
            }

            MachineImage img;

            String osId = server.getTag("guestosid").toString();
            String name = validateName(options.getName());

            List<Param> params = new ArrayList<Param>();
            params.add(new Param("name", name));
            params.add(new Param("displayText", name));
            params.add(new Param("osTypeId", osId == null ? toOs(server.getPlatform(),server.getArchitecture()) : osId));
            params.add(new Param("zoneId", getContext().getRegionId()));
            params.add(new Param("isPublic", "false"));
            params.add(new Param("isFeatured", "false"));
            params.add(new Param("volumeid",rootVolumeId));
            params.add(new Param("passwordEnabled", String.valueOf(server.getTag("passwordenabled"))));
            doc = method.get(method.buildUrl(CREATE_TEMPLATE, params), CREATE_TEMPLATE);

            NodeList matches = doc.getElementsByTagName("templateid"); // v2.1
            String templateId = null;

            if( matches.getLength() > 0 ) {
                templateId = matches.item(0).getFirstChild().getNodeValue();
            }
            if( templateId == null ) {
                matches = doc.getElementsByTagName("id"); // v2.2
                if( matches.getLength() > 0 ) {
                    templateId = matches.item(0).getFirstChild().getNodeValue();
                }
            }
            if( templateId == null ) {
                matches = doc.getElementsByTagName("jobid"); // v4.1
                if( matches.getLength() > 0 ) {
                    templateId = matches.item(0).getFirstChild().getNodeValue();
                }
            }
            if( templateId == null ) {
                throw new CloudException("Failed to provide a template ID.");
            }
            Document responseDoc = getProvider().waitForJob(doc, "Create Template");
            if (responseDoc != null){
                NodeList nodeList = responseDoc.getElementsByTagName("template");
                if (nodeList.getLength() > 0) {
                    Node template = nodeList.item(0);
                    NodeList attributes = template.getChildNodes();
                    for (int i = 0; i<attributes.getLength(); i++) {
                        Node attribute = attributes.item(i);
                        String tmpname = attribute.getNodeName().toLowerCase();
                        String value;

                        if( attribute.getChildNodes().getLength() > 0 ) {
                            value = attribute.getFirstChild().getNodeValue();
                        }
                        else {
                            value = null;
                        }
                        if (tmpname.equalsIgnoreCase("id")) {
                            templateId = value;
                            break;
                        }
                    }
                }
            }
            img = getImage(templateId);
            if( img == null ) {
                throw new CloudException("Machine image job completed successfully, but no image " + templateId + " exists.");
            }
            if( task != null ) {
                task.completeWithResult(img);
            }
            if (restart) {
                getProvider().getComputeServices().getVirtualMachineSupport().start(vmId);
            }
            return img;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public boolean isImageSharedWithPublic(@Nonnull String templateId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.isImageSharedWithPublic");
        try {
            CSMethod method = new CSMethod(getProvider());
            String url = method.buildUrl(LIST_TEMPLATES, new Param("templateFilter", "executable"), new Param("id", templateId));
            Document doc;
            boolean isTemplate = true;

            try {
                doc = method.get(url, LIST_TEMPLATES);
            }
            catch( CSException e ) {
                if( e.getHttpCode() == 431 ) {
                    //check if we can find a match to iso with this id
                    url = method.buildUrl(LIST_ISOS, new Param("id", templateId), new Param("isoFilter", "executable"), new Param("bootable", "true"));
                    doc = method.get(url, LIST_ISOS);
                    isTemplate = false;
                }
                else {
                    throw e;
                }
            }
            NodeList matches;
            if (isTemplate) {
                matches = doc.getElementsByTagName("template");
            }
            else {
                matches = doc.getElementsByTagName("iso");
            }

            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);

                MachineImage image = toImage(node, true, null);

                if( image != null && image.getProviderMachineImageId().equals(templateId) ) {
                    return true;
                }
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }
    
    private boolean isPasswordEnabled(@Nonnull String templateId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Image.isPasswordEnabled");
        try {
            CSMethod method = new CSMethod(getProvider());
            String url = method.buildUrl(LIST_TEMPLATES, new Param("templateFilter", "executable"), new Param("id", templateId));
            Document doc;
            boolean isTemplate = true;

            try {
                doc = method.get(url, LIST_TEMPLATES);
            }
            catch( CSException e ) {
                if( e.getHttpCode() == 431 ) {
                    //check if we can find a match to iso with this id
                    url = method.buildUrl(LIST_ISOS, new Param("id", templateId), new Param("isoFilter", "executable"), new Param("bootable", "true"));
                    doc = method.get(url, LIST_ISOS);
                    isTemplate = false;
                }
                else {
                    throw e;
                }
            }
            NodeList matches;
            if (isTemplate) {
                matches = doc.getElementsByTagName("template");
            }
            else {
                matches = doc.getElementsByTagName("iso");
            }

            if( matches.getLength() > 0 ) {
                Node node = matches.item(0);

                Boolean val = isPasswordEnabled(templateId, node);

                return (val != null && val);
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }
    
    private @Nullable Boolean isPasswordEnabled(@Nonnull String templateId, @Nullable Node node) {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        boolean enabled = false;
        String id = null;
        
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
                id = value;
            }
            else if( name.equalsIgnoreCase("passwordenabled") ) {
                enabled = (value != null && value.equalsIgnoreCase("true"));
            }
            if( id != null && enabled ) {
                break;
            }
        } 
        if( id == null || !id.equals(templateId) ) {
            return null;
        }
        return enabled;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.isSubscribed");
        try {
            CSMethod method = new CSMethod(getProvider());

            try {
                method.get(method.buildUrl(CSTopology.LIST_ZONES, new Param("available", "true")), CSTopology.LIST_ZONES);
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listImageStatus");
        try {
            if( !cls.equals(ImageClass.MACHINE) ) {
                return Collections.emptyList();
            }
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(LIST_TEMPLATES, new Param("templateFilter", "self"), new Param("zoneId", ctx.getRegionId())), LIST_TEMPLATES);
            List<ResourceStatus> templates = new ArrayList<ResourceStatus>();

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
                    doc = method.get(method.buildUrl(LIST_TEMPLATES, new Param("templateFilter", "self"), new Param("zoneId", ctx.getRegionId()), new Param("pagesize", "500"), new Param("page", nextPage)), LIST_TEMPLATES);
                }
                NodeList matches = doc.getElementsByTagName("template");

                for( int i=0; i<matches.getLength(); i++ ) {
                    ResourceStatus status = toStatus(matches.item(i), false);

                    if( status != null ) {
                        templates.add(status);
                    }
                }
            }
            //todo add iso status once we have support for launching from them
            //templates.addAll(listIsoStatus());
            return templates;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull List<ResourceStatus> listIsoStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listImageStatus");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(LIST_ISOS, new Param("isoFilter", "self"), new Param("zoneId", ctx.getRegionId()), new Param("bootable", "true")), LIST_ISOS);
            List<ResourceStatus> templates = new ArrayList<ResourceStatus>();

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
                    doc = method.get(method.buildUrl(LIST_ISOS, new Param("isoFilter", "self"), new Param("zoneId", ctx.getRegionId()), new Param("bootable", "true"), new Param("pagesize", "500"), new Param("page", nextPage)), LIST_ISOS);
                }
                NodeList matches = doc.getElementsByTagName("iso");

                for( int i=0; i<matches.getLength(); i++ ) {
                    ResourceStatus status = toStatus(matches.item(i), false);

                    if( status != null ) {
                        templates.add(status);
                    }
                }
            }
            return templates;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listImages");
        try {
            List<String> hypervisors = getProvider().getZoneHypervisors(getContext().getRegionId());

            CSMethod method = new CSMethod(getProvider());
            String accountNumber = (options == null ? null : options.getAccountNumber());
            List<Param> params = new ArrayList<Param>();
            if( getContext().getRegionId() != null && !getContext().getRegionId().isEmpty() ) {
                params.add(new Param("zoneId", getContext().getRegionId()));
            }
            params.add(new Param("pagesize", "500"));

            // if only single hypervisor is supported by zone, let's limit to that
            if( hypervisors != null && hypervisors.size() == 1 ) {
                params.add(new Param("hypervisor", hypervisors.get(0)));
            }

            String templateFilter = "executable";
            if( accountNumber == null || getProvider().getServiceProvider().equals(CSServiceProvider.DATAPIPE) ) {
                templateFilter = "selfexecutable";
            }
            params.add(new Param("templateFilter", templateFilter));
            Param pageParam = new Param("page", "1");
            params.add(pageParam);

            Document doc = method.get(method.buildUrl(LIST_ISOS, params), LIST_ISOS);

            List<MachineImage> templates = new ArrayList<MachineImage>();

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
                    pageParam.setValue(nextPage);
                    doc = method.get(method.buildUrl(LIST_TEMPLATES, params), LIST_TEMPLATES);
                }
                NodeList matches = doc.getElementsByTagName("iso");

                for( int i=0; i<matches.getLength(); i++ ) {
                    MachineImage image = toImage(matches.item(i), false, hypervisors);

                    if( image != null && (options == null || options.matches(image)) ) {
                        image.setTag("isISO", "true");
                        templates.add(image);
                    }
                }
            }

            //todo list isos too once we have support for launching from them
            //templates.addAll(listIsos(options));

            return templates;
        }
        finally {
            APITrace.end();
        }
    }

    // TODO: refactor to use listImages, as the code is a copy-paste with minor changes.
    private @Nonnull ArrayList<MachineImage> listIsos(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listIsos");
        try {
            CSMethod method = new CSMethod(getProvider());
            String accountNumber = (options == null ? null : options.getAccountNumber());
            Param[] params;

            if( accountNumber == null || getProvider().getServiceProvider().equals(CSServiceProvider.DATAPIPE) ) {
                params = new Param[] { new Param("isoFilter", "selfexecutable"),  new Param("zoneId", getContext().getRegionId()), new Param("bootable", "true"), new Param("pagesize", "500"), new Param("page", "1") };
            }
            else {
                String domainId = provider.getDomainId(accountNumber);
                String parentAccount = provider.getParentAccount(accountNumber);
                params = new Param[] { new Param("isoFilter", "executable"),  new Param("zoneId", getContext().getRegionId()), new Param("account", parentAccount), new Param("domainId", domainId), new Param("bootable", "true"), new Param("pagesize", "500"), new Param("page", "1") };
            }

            Document doc = method.get(method.buildUrl(LIST_ISOS, params), LIST_ISOS);

            ArrayList<MachineImage> templates = new ArrayList<MachineImage>();

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
                    int length = params.length;
                    params[length-1] = new Param("page", nextPage);
                    doc = method.get(method.buildUrl(LIST_ISOS, params), LIST_ISOS);
                }
                NodeList matches = doc.getElementsByTagName("iso");

                for( int i=0; i<matches.getLength(); i++ ) {
                    MachineImage image = toImage(matches.item(i), false, getProvider().getZoneHypervisors(getContext().getRegionId()));


                    if( image != null && (options == null || options.matches(image)) ) {
                        image.setTag("isISO", "true");
                        templates.add(image);
                    }
                }
            }

            return templates;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String templateId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listShares");
        try {
            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(LIST_TEMPLATE_PERMISSIONS, new Param("id", templateId)), LIST_TEMPLATES);
            }
            catch( CSException e ) {
                if( e.getHttpCode() == 431 ) {
                    //check if we can find a match to iso with this id
                    doc = method.get(method.buildUrl(LIST_ISO_PERMISSIONS, new Param("id", templateId)), LIST_ISO_PERMISSIONS);
                }
                else {
                    throw e;
                }
            }
            Set<String> accounts = new TreeSet<String>();
            NodeList matches = doc.getElementsByTagName("account");

            for( int i=0; i<matches.getLength(); i++ ) {
                Node node = matches.item(i);

                accounts.add(node.getFirstChild().getNodeValue());
            }
            return accounts;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.registerImageBundle");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String atStorageLocation = options.getBundleLocation();

            if( atStorageLocation == null ) {
                throw new OperationNotSupportedException("Cannot register a machine image without a location");
            }
            String name = validateName(options.getName());

            Platform platform = Platform.guess(name);
            Architecture architecture = guess(name);
            List<Param> params = new ArrayList<Param>();

            params.add(new Param("name", name));
            params.add(new Param("displayText", name));
            params.add(new Param("url", atStorageLocation));
            MachineImageFormat fmt = options.getBundleFormat();

            if( fmt == null ) {
                throw new CloudException("You must specify the bundle format for the new bundle");
            }
            String format;
            switch( options.getBundleFormat() ) {
                case VHD:
                    format = "VHD"; break;
                case RAW:
                    format = "RAW"; break;
                case QCOW2:
                    format = "QCOW2"; break;
                default:
                    throw new OperationNotSupportedException("Unsupported bundle format: " + options.getBundleFormat());
            }
            params.add(new Param("format", format));

            params.add(new Param("osTypeId", toOs(platform, architecture)));
            params.add(new Param("zoneId", ctx.getRegionId()));
            params.add(new Param("isPublic", "false"));
            params.add(new Param("isFeatured", "false"));

            CSMethod method = new CSMethod(getProvider());
            Document doc = method.get(method.buildUrl(REGISTER_TEMPLATE, params), REGISTER_TEMPLATE);
            NodeList matches = doc.getElementsByTagName("templateid");
            String templateId = null;

            if( matches.getLength() > 0 ) {
                templateId = matches.item(0).getFirstChild().getNodeValue();
            }
            else {
                matches = doc.getElementsByTagName("id");
                if (matches.getLength() > 0) {
                    templateId = matches.item(0).getFirstChild().getNodeValue();
                }
            }
            if( templateId == null ) {
                throw new CloudException("No error was encountered during registration, but no templateId was returned");
            }
            getProvider().waitForJob(doc, "Create Template");
            MachineImage img = getImage(templateId);

            if( img == null ) {
                throw new CloudException("Machine image " + templateId + " was created, but it does not exist");
            }
            return img;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.remove");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for the request");
            }
            String regionId = ctx.getRegionId();
            String accountNumber = ctx.getAccountNumber();
            MachineImage img = getImage(providerImageId);

            if( img == null ) {
                throw new CloudException("No such machine image: " + providerImageId);
            }
            if( !accountNumber.equals(img.getProviderOwnerId())
                      && !getProvider().getParentAccount().equalsIgnoreCase(img.getProviderOwnerId())) {
                throw new CloudException(accountNumber + " cannot remove images belonging to " + img.getProviderOwnerId());
            }
            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(DELETE_TEMPLATE, new Param("id", providerImageId), new Param("zoneid", regionId)), DELETE_TEMPLATE);
                getProvider().waitForJob(doc, "Delete Template");
            }
            catch (CSException e) {
                if (e.getHttpCode()==431) {
                    //try update iso share
                    doc = method.get(method.buildUrl(DELETE_ISO, new Param("id", providerImageId)), DELETE_ISO);
                    getProvider().waitForJob(doc, "Delete Iso");
                }
                else {
                    throw e;
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removeAllImageShares");
        try {
            for( String account : listShares(providerImageId) ) {
                removeImageShare(providerImageId, account);
            }
            removePublicShare(providerImageId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removeImageShare");
        try {
            MachineImage img = getImage(providerImageId);

            if( img == null ) {
                return;
            }
            if( !getContext().getAccountNumber().equals(img.getProviderOwnerId())
                    && !getProvider().getParentAccount().equalsIgnoreCase(img.getProviderOwnerId())) {
                return;
            }
            Param[] params = new Param[] { new Param("id", providerImageId), new Param("accounts", accountNumber), new Param("op", "remove") };

            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(UPDATE_TEMPLATE_PERMISSIONS, params), UPDATE_TEMPLATE_PERMISSIONS);
                getProvider().waitForJob(doc, "Share Template");
            }
            catch (CSException e) {
                if (e.getHttpCode()==431) {
                    //try update iso share
                    doc = method.get(method.buildUrl(UPDATE_ISO_PERMISSIONS, params), UPDATE_ISO_PERMISSIONS);
                    getProvider().waitForJob(doc, "Share Iso");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.removePublicShare");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            MachineImage img = getImage(providerImageId);

            if( img == null ) {
                return;
            }
            if( !ctx.getAccountNumber().equals(img.getProviderOwnerId())
                    && !getProvider().getParentAccount().equalsIgnoreCase(img.getProviderOwnerId())) {
                return;
            }
            Param[] params = new Param[] { new Param("id", providerImageId), new Param("isPublic", "false") };

            CSMethod method = new CSMethod(getProvider());
            Document doc;
            try {
                doc = method.get(method.buildUrl(UPDATE_TEMPLATE_PERMISSIONS, params), UPDATE_TEMPLATE_PERMISSIONS);
                getProvider().waitForJob(doc, "Share Template");
            }
            catch (CSException e) {
                if (e.getHttpCode()==431) {
                    //try update iso share
                    doc = method.get(method.buildUrl(UPDATE_ISO_PERMISSIONS, params), UPDATE_ISO_PERMISSIONS);
                    getProvider().waitForJob(doc, "Share Iso");
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(final @Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        //dmayne 20131004: need to get both sets of filters (featured and community) to match direct console
        final List<Param> params = new ArrayList<Param>();
        final List<MachineImage> allImages = new ArrayList<MachineImage>();
        final CSMethod method = new CSMethod(getProvider());
        Param filterParam = new Param("templateFilter", "featured");
        params.add(filterParam);
        final List<String> hypervisors = getProvider().getZoneHypervisors(getContext().getRegionId());
        if( hypervisors != null && hypervisors.size() == 1) {
            params.add(new Param("hypervisor", hypervisors.get(0)));
        }
        if( getContext().getRegionId() != null && !getContext().getRegionId().isEmpty() ) {
            params.add(new Param("zoneId", getContext().getRegionId()));
        }

        //todo add public isos when we can support launching vms from them
        //todo refactor to make smaller methods
        // params3 = new Param[] { new Param("isoFilter", "featured"),  new Param("zoneId", getContext().getRegionId()), new Param("bootable", "true") };
        // params4 = new Param[] { new Param("isoFilter", "community"),  new Param("zoneId", getContext().getRegionId()), new Param("bootable", "true") };


        getProvider().hold();
        PopulatorThread<MachineImage> populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            @Override
            public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                try {
                    APITrace.begin(getProvider(), "Image.searchPublicImages.populate");
                    try {
                        Document doc = method.get(method.buildUrl(LIST_TEMPLATES, params), LIST_TEMPLATES);
                        NodeList matches = doc.getElementsByTagName("template");

                        for( int i=0; i<matches.getLength(); i++ ) {
                            MachineImage img = toImage(matches.item(i), true, hypervisors);

                            if( img != null && options.matches(img) ) {
                                iterator.push(img);
                            }
                        }
                    }
                    finally {
                        APITrace.end();
                    }
                }
                finally {
                    getProvider().release();
                }
            }
        });

        populator.populate();
        allImages.addAll(populator.getResult());

        /*todo add public isos when we can support launching vms from them
         getProvider().hold();
         populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
             @Override
             public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                 try {
                     APITrace.begin(getProvider(), "Image.searchPublicImages.populate");
                     try {
                         Document doc = method.get(method.buildUrl(LIST_ISOS, params3), LIST_ISOS);
                         NodeList matches = doc.getElementsByTagName("iso");

                         for( int i=0; i<matches.getLength(); i++ ) {
                             MachineImage img = toImage(matches.item(i), true);


                             if( img != null && options.matches(img) ) {
                                 img.setTag("isISO", "true");
                                 iterator.push(img);
                             }
                         }
                     }
                     finally {
                         APITrace.end();
                     }
                 }
                 finally {
                     getProvider().release();
                 }
             }
         });

         populator.populate();
         allImages.addAll(populator.getResult());
         */
        filterParam.setValue("community");
        if (!getProvider().getServiceProvider().equals(CSServiceProvider.DATAPIPE) ) {
            getProvider().hold();
            populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
                @Override
                public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                    try {
                        APITrace.begin(getProvider(), "Image.searchPublicImages.populate");
                        try {
                            Document doc = method.get(method.buildUrl(LIST_TEMPLATES, params), LIST_TEMPLATES);
                            NodeList matches = doc.getElementsByTagName("template");

                            for( int i=0; i<matches.getLength(); i++ ) {
                                MachineImage img = toImage(matches.item(i), true, hypervisors);

                                if( img != null && options.matches(img) && !allImages.contains(img)) {
                                    iterator.push(img);
                                }
                            }
                        }
                        finally {
                            APITrace.end();
                        }
                    }
                    finally {
                        getProvider().release();
                    }
                }
            });

            populator.populate();
            allImages.addAll(populator.getResult());

            /*todo add public isos when we can support launching vms from them
             getProvider().hold();
             populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
                 @Override
                 public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                     try {
                         APITrace.begin(getProvider(), "Image.searchPublicImages.populate");
                         try {
                             Document doc = method.get(method.buildUrl(LIST_ISOS, params4), LIST_ISOS);
                             NodeList matches = doc.getElementsByTagName("iso");

                             for( int i=0; i<matches.getLength(); i++ ) {
                                 MachineImage img = toImage(matches.item(i), true);


                                 if( img != null && options.matches(img) && !allImages.contains(img)) {
                                    img.setTag("isISO", "true");
                                    iterator.push(img);
                                }
                            }
                        }
                        finally {
                            APITrace.end();
                        }
                    }
                    finally {
                        getProvider().release();
                    }
                }
            });

            populator.populate();
            allImages.addAll(populator.getResult());
            */
        }
        return allImages;
    }

    private @Nullable MachineImage toImage(@Nullable Node node, boolean onlyIfPublic, List<String> desiredHypervisors) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        Architecture bestArchitectureGuess = Architecture.I64;
        Map<String, String> properties = new HashMap<String,String>();
        NodeList attributes = node.getChildNodes();
        boolean isPublic = false;

        String providerOwnerId = getContext().getAccountNumber();
        MachineImageState state = MachineImageState.PENDING;
        String regionId = null;
        ImageClass imageClass = ImageClass.MACHINE;
        String imageId = null, imgName = null, description = null;
        Platform platform = null;
        Architecture architecture = null;
        long creationTimestamp = 0l;
        boolean crossZones = false;
        
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName().toLowerCase();
            String value = null;
            if( attribute.hasChildNodes() && attribute.getChildNodes().getLength() > 0 ) {
                value = attribute.getFirstChild().getNodeValue();
            }
            if( name.equals("id") ) {
                imageId = value;
            }
            else if( name.equals("crosszones") ) {
                crossZones = Boolean.valueOf(value);
            }
            else if( name.equals("zoneid") ) {
                regionId = value;
            }
            else if( name.equals("hypervisor") ) {
                // check if the image hypervisor is not in the desired list
                if( desiredHypervisors != null && !desiredHypervisors.contains(value) ) {
                    return null;
                }
            }
            else if( name.equalsIgnoreCase("account") ) {
                providerOwnerId = value;
            }
            else if( name.equals("name") ) {
                imgName = value;
                if( value != null && value.contains("x64") ) {
                    bestArchitectureGuess = Architecture.I64;
                }
                else if( value != null && value.contains("x32") ) {
                    bestArchitectureGuess = Architecture.I32;
                }
            }
            else if( name.equals("displaytext") ) {
                description = value;
                if( value != null && value.contains("x64") ) {
                    bestArchitectureGuess = Architecture.I64;
                }
                else if( value != null && value.contains("x32") ) {
                    bestArchitectureGuess = Architecture.I32;
                }
            }
            else if( name.equals("ispublic") ) {
                isPublic = (value != null && value.equalsIgnoreCase("true"));
            }
            else if( name.equals("ostypename") ) {
                if( value != null && value.contains("64") ) {
                    bestArchitectureGuess = Architecture.I64;
                }
                else if( value != null && value.contains("32") ) {
                    bestArchitectureGuess = Architecture.I32;
                }
                if( value != null ) {
                    platform = Platform.guess(value);
                }
            }
            else if( name.equals("ostypeid") && value != null ) {
                properties.put("cloud.com.os.typeId", value);
            }
            else if( name.equals("bits") ) {
                if( value == null || value.equals("64") ) {
                    architecture = Architecture.I64;
                }
                else {
                    architecture = Architecture.I32;
                }
            }
            else if( name.equals("created") ) {
                // 2010-06-29T20:49:28+1000
                if( value != null ) {
                    creationTimestamp = getProvider().parseTime(value);
                }
            }
            else if( name.equals("isready") ) {
                if( value != null && value.equalsIgnoreCase("true") ) {
                    state = (MachineImageState.ACTIVE);
                }
            }
            else if( name.equals("status") ) {
                if( value == null || !value.equalsIgnoreCase("Download Complete") ) {
                    logger.warn("Template status=" + value);
                }
            }
        }
        if( platform == null && imgName != null ) {
            platform = (Platform.guess(imgName));
        }
        if (platform == null) {
            platform = Platform.UNKNOWN;
        }
        
        if( architecture == null ) {
            architecture = bestArchitectureGuess;
        }
        MachineImage image = null;
        if( !onlyIfPublic || isPublic ) {
            image = MachineImage.getImageInstance(providerOwnerId, regionId, imageId, imageClass, state, imgName, description, architecture, platform);
            guessSoftware(image);
            image.setTags(properties);
            image.createdAt(creationTimestamp);
        }
        // if image is not available in all zones and image zone is not matching requested - bail out
        if( !crossZones && !regionId.equalsIgnoreCase(getContext().getRegionId()) ) {
            return null;
        }
        return image;
    }

    private @Nullable ResourceStatus toStatus(@Nullable Node node, boolean onlyIfPublic) throws CloudException, InternalException {
        if( node == null ) {
            return null;
        }
        NodeList attributes = node.getChildNodes();
        MachineImageState imageState = null;
        String imageId = null;
        boolean isPublic = false;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName().toLowerCase();
            String value;

            if( attribute.hasChildNodes() && attribute.getChildNodes().getLength() > 0 ) {
                value = attribute.getFirstChild().getNodeValue();
            }
            else {
                value = null;
            }
            if( name.equals("id") ) {
                imageId = value;
            }
            else if( name.equals("ispublic") ) {
                isPublic = (value != null && value.equalsIgnoreCase("true"));
            }
            else if( name.equals("isready") ) {
                if( value != null && value.equalsIgnoreCase("true") ) {
                    imageState = MachineImageState.ACTIVE;
                }
            }
        }
        if( imageId != null && (!onlyIfPublic || isPublic) ) {
            if( imageState == null ) {
                imageState = MachineImageState.PENDING;
            }
            return new ResourceStatus(imageId, imageState);
        }
        return null;
    }

    private String toOs(Platform platform, Architecture architecture) throws InternalException, CloudException {
        CSMethod method = new CSMethod(getProvider());
        Document doc = method.get(method.buildUrl(LIST_OS_TYPES), LIST_OS_TYPES);
        NodeList matches = doc.getElementsByTagName("ostype");
        
        for( int i=0; i<matches.getLength(); i++ ) {
            NodeList attrs = matches.item(i).getChildNodes();
            Architecture arch = Architecture.I64;
            Platform pf = null;
            String id = null;
            
            for( int j=0; j<attrs.getLength(); j++ ) {
                Node attr = attrs.item(j);

                if( attr.getNodeName().equals("id") ) {
                    id = attr.getFirstChild().getNodeValue();
                }
                else if( attr.getNodeName().equals("description") ) {
                    String desc = attr.getFirstChild().getNodeValue();
                    
                    pf = Platform.guess(desc);
                    arch = guess(desc);
                }
            }
            if( platform.equals(pf) && architecture.equals(arch) ) {
                return id;
            }
        }
        return null;
    }
    
    private String validateName(String name) throws InternalException, CloudException {
        if( name.length() < 32 ) {
            return name;
        }
        name = name.substring(0,32);
        boolean found;
        int i = 0;
        
        do {
            found = false;
            for( MachineImage vm : listImages(ImageClass.MACHINE) ) {
                if( name.equals(vm.getName()) ) {
                    found = true;
                    break;
                }
            }
            if( found ) {
                i++;
                if( i < 10 ) {
                    name = name.substring(0,31) + i;
                }
                else if( i < 100 ) {
                    name = name.substring(0, 30) + i;
                }
                else {
                    name = name.substring(0, 29) + i;
                }
            }
        } while( found );
        return name;
    }
}
