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

package org.dasein.cloud.cloudstack.identity;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.cloudstack.CSMethod;
import org.dasein.cloud.cloudstack.Param;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeySupport;
import org.dasein.cloud.util.APITrace;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

/**
 * Implements the CSCloud 3.0 SSH keypair support
 * @author George Reese
 * @since 2012.02
 * @version 2012.02
 */
public class Keypair implements ShellKeySupport {
    private CSCloud provider;
    
    Keypair(@Nonnull CSCloud provider) { this.provider = provider; }

    @Override
    public @Nonnull SSHKeypair createKeypair(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.createKeypair");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            CSMethod method = new CSMethod(provider);
            Document doc = method.get(method.buildUrl(CSMethod.CREATE_KEYPAIR, new Param("name", name)), CSMethod.CREATE_KEYPAIR);
            NodeList matches = doc.getElementsByTagName("keypair");

            for( int i=0; i<matches.getLength(); i++ ) {
                SSHKeypair key = toKeypair(ctx, matches.item(i));

                if( key != null ) {
                    return key;
                }
            }
            throw new CloudException("Request did not error, but no keypair was generated");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteKeypair(@Nonnull String providerId) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.deleteKeypair");
        try {
            CSMethod method = new CSMethod(provider);

            method.get(method.buildUrl(CSMethod.DELETE_KEYPAIR, new Param("name", providerId)), CSMethod.DELETE_KEYPAIR);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable String getFingerprint(@Nonnull String providerId) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.getFingerprint");
        try {
            SSHKeypair keypair = getKeypair(providerId);

            return (keypair == null ? null : keypair.getFingerprint());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Requirement getKeyImportSupport() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nullable SSHKeypair getKeypair(@Nonnull String providerId) throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.getKeypair");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            CSMethod method = new CSMethod(provider);
            Document doc = method.get(method.buildUrl(CSMethod.LIST_KEYPAIRS, new Param("name", providerId)), CSMethod.LIST_KEYPAIRS);
            NodeList matches = doc.getElementsByTagName("sshkeypair");

            for( int i=0; i<matches.getLength(); i++ ) {
                SSHKeypair key = toKeypair(ctx, matches.item(i));

                if( key != null ) {
                    return key;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForKeypair(@Nonnull Locale locale) {
        return "SSH keypair";
    }

    @Override
    public @Nonnull SSHKeypair importKeypair(@Nonnull String name, @Nonnull String publicKey) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Import of keypairs is not supported");
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "Keypair.isSubscribed");
        try {
            return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<SSHKeypair> list() throws InternalException, CloudException {
        APITrace.begin(provider, "Keypair.list");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            CSMethod method = new CSMethod(provider);
            Document doc = method.get(method.buildUrl(CSMethod.LIST_KEYPAIRS), CSMethod.LIST_KEYPAIRS);
            ArrayList<SSHKeypair> keys = new ArrayList<SSHKeypair>();

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
                    doc = method.get(method.buildUrl(CSMethod.LIST_KEYPAIRS, new Param("pagesize", "500"), new Param("page", nextPage)), CSMethod.LIST_KEYPAIRS);
                }
                NodeList matches = doc.getElementsByTagName("sshkeypair");

                for( int i=0; i<matches.getLength(); i++ ) {
                    SSHKeypair key = toKeypair(ctx, matches.item(i));

                    if( key != null ) {
                        keys.add(key);
                    }
                }
            }
            return keys;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable SSHKeypair toKeypair(@Nonnull ProviderContext ctx, @Nullable Node node) throws CloudException, InternalException {
        if( node == null || !node.hasChildNodes() ) {
            return null;
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region is part of this request");
        }
        NodeList attributes = node.getChildNodes();
        SSHKeypair kp = new SSHKeypair();
        String privateKey = null;
        String fingerprint = null;
        String name = null;
        
        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            
            if( attribute != null ) {
                String nodeName = attribute.getNodeName();
                
                if( nodeName.equalsIgnoreCase("name") && attribute.hasChildNodes() ) {
                    name = attribute.getFirstChild().getNodeValue().trim(); 
                }
                else if( nodeName.equalsIgnoreCase("fingerprint") && attribute.hasChildNodes() ) {
                    fingerprint = attribute.getFirstChild().getNodeValue().trim();
                }
                else if( nodeName.equalsIgnoreCase("privatekey") && attribute.hasChildNodes() ) {
                    privateKey = attribute.getFirstChild().getNodeValue().trim();
                }
            }
        }
        if( name == null || fingerprint == null ) {
            return null;
        }
        kp.setProviderRegionId(regionId);
        kp.setProviderOwnerId(ctx.getAccountNumber());
        kp.setProviderKeypairId(name);
        kp.setName(name);
        kp.setFingerprint(fingerprint);
        if( privateKey != null ) {
            try {
                kp.setPrivateKey(privateKey.getBytes("utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
        }
        return kp;
    }
}
