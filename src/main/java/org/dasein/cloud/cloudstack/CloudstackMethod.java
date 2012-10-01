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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.SignatureException;
import java.util.Date;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class CloudstackMethod { 
    static public class ParsedError {
        public int code;
        public String message;
    }
    
    static public final String CREATE_KEYPAIR = "createSSHKeyPair";
    static public final String DELETE_KEYPAIR = "deleteSSHKeyPair";
    static public final String LIST_KEYPAIRS  = "listSSHKeyPairs";

    private CloudstackProvider provider;
    
    public CloudstackMethod(@Nonnull CloudstackProvider provider) { this.provider = provider; }
    
    public String buildUrl(String command, Param ... params) throws InternalException {
        try {
            StringBuilder str = new StringBuilder();
            String apiKey = new String(provider.getContext().getAccessPublic(), "utf-8");
            String accessKey = new String(provider.getContext().getAccessPrivate(), "utf-8");

            StringBuilder newKey = new StringBuilder();
            for( int i =0; i<apiKey.length(); i++ ) {
                char c = apiKey.charAt(i);
                
                if( c != '\r' ) {
                    newKey.append(c);
                }
            }
            apiKey = newKey.toString();
            newKey = new StringBuilder();
            for( int i =0; i<accessKey.length(); i++ ) {
                char c = accessKey.charAt(i);
                
                if( c != '\r' ) {
                    newKey.append(c);
                }
            }
            accessKey = newKey.toString();
            str.append(provider.getContext().getEndpoint());
            str.append("/api?command=");
            str.append(command);
            for( Param param : params ) {
                str.append("&");
                str.append(param.getKey());
                str.append("=");
                str.append(URLEncoder.encode(param.getValue(), "UTF-8").replaceAll("\\+", "%20"));
            }
            str.append("&apiKey=");
            str.append(URLEncoder.encode(apiKey, "UTF-8").replaceAll("\\+", "%20"));
            str.append("&signature=");
            try {
                str.append(URLEncoder.encode(getSignature(command, apiKey, accessKey, params), "UTF-8").replaceAll("\\+", "%20"));
            }
            catch( SignatureException e ) {
                throw new InternalException(e);
            }
            return str.toString();
        }
        catch( UnsupportedEncodingException e ) {
            e.printStackTrace();
            throw new RuntimeException("This cannot happen: " + e.getMessage());
        }
    }
    
    private byte[] calculateHmac(String data, String key) throws SignatureException {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(), "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(signingKey);
        
            return mac.doFinal(data.getBytes());
        } 
        catch (Exception e) {
            throw new SignatureException("Failed to generate HMAC : " + e.getMessage());
        }
    }
    
    public Document get(String url) throws CloudException, InternalException {
        Logger wire = CloudstackProvider.getLogger(CloudstackMethod.class, "wire");
        Logger logger = CloudstackProvider.getLogger(CloudstackMethod.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudstackMethod.class.getName() + ".get(" + url + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] -------------------------------------------------------------------");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(url);
            int code;
            
            get.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath() + "?" + get.getQueryString());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            try {
                code = client.executeMethod(get);
            }
            catch( HttpException e ) {
                throw new InternalException("HttpException during GET: " + e.getMessage());
            }
            catch( IOException e ) {
                throw new CloudException("IOException during GET: " + e.getMessage());
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("get(): HTTP Status " + code);
            }
            if( wire.isDebugEnabled() ) {
                Header[] headers = get.getResponseHeaders();
                
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            try {
                if( code != HttpStatus.SC_OK ) {
                    String body = get.getResponseBodyAsString();
                    
                    if( body.contains("<html>") ) {
                        if( code == HttpStatus.SC_FORBIDDEN || code == HttpStatus.SC_UNAUTHORIZED ) {
                            CloudstackMethod.ParsedError p = new CloudstackMethod.ParsedError();
                            
                            p.code = code;
                            p.message = body;
                            throw new CloudstackException(CloudErrorType.AUTHENTICATION, p);
                        }
                        else if( code == 430 || code == 431 || code == 432 || code == 436 ) {
                            return null;
                        }
                        CloudstackMethod.ParsedError p = new CloudstackMethod.ParsedError();
                        
                        p.code = code;
                        p.message = body;
                        throw new CloudstackException(p);
                    }
                    throw new CloudstackException(parseError(code, body));
                }
                return parseResponse(code, get.getResponseBodyAsString());
            }
            catch( IOException e ) {
                throw new CloudException("IOException getting stream: " + e.getMessage());
            }         
        }
        finally {
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -------------------------------------------------------------------");
            }
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudstackMethod.class.getName() + ".get()");
            }            
        }
    }
    
    protected HttpClient getClient() {
        String proxyHost = provider.getContext().getCustomProperties().getProperty("proxyHost");
        String proxyPort = provider.getContext().getCustomProperties().getProperty("proxyPort");
        HttpClient client = new HttpClient();
        
        if( proxyHost != null ) {
            int port = 0;
            
            if( proxyPort != null && proxyPort.length() > 0 ) {
                port = Integer.parseInt(proxyPort);
            }
            client.getHostConfiguration().setProxy(proxyHost, port);
        }
        return client;
    }
    
    private String getSignature(String command, String apiKey, String accessKey, Param ... params) throws UnsupportedEncodingException, SignatureException {
        Logger logger = CloudstackProvider.getLogger(CloudstackMethod.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudstackMethod.class.getName() + ".getSignature(" + command + "," + apiKey + "," + accessKey + "," + params + ")");
        }
        try {
            TreeSet<Param> sorted = new TreeSet<Param>();
            StringBuilder str = new StringBuilder();
            
            sorted.add(new Param("command", URLEncoder.encode(command, "UTF-8").replaceAll("\\+", "%20").toLowerCase()));
            sorted.add(new Param("apikey", URLEncoder.encode(apiKey, "UTF-8").replaceAll("\\+", "%20").toLowerCase()));
            for( Param param : params ) {
                sorted.add(new Param(param.getKey().toLowerCase(), URLEncoder.encode(param.getValue(), "UTF-8").replaceAll("\\+", "%20").toLowerCase()));
            }
            boolean first = true;
            for( Param param : sorted ) {
                if( !first ) {
                    str.append("&");
                }
                first = false;
                str.append(param.getKey());
                str.append("=");
                str.append(param.getValue());
            }
            if( logger.isDebugEnabled()  ) { 
                logger.debug("getSignature(): String to sign=" + str.toString());
            }
            return new String(Base64.encodeBase64(calculateHmac(str.toString(), accessKey)));
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudstackMethod.class.getName() + ".getSignature()");
            }
        }
    }
    
    private ParsedError parseError(int httpStatus, String assumedXml) throws InternalException {
        Logger logger = CloudstackProvider.getLogger(CloudstackMethod.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudstackMethod.class.getName() + ".parseError(" + httpStatus + "," + assumedXml + ")");
        }
        try {
            ParsedError error = new ParsedError();
            
            error.code = httpStatus;
            error.message = null;
            try {
                Document doc = parseResponse(httpStatus, assumedXml);
                
                NodeList codes = doc.getElementsByTagName("errorcode");
                for( int i=0; i<codes.getLength(); i++ ) {
                    Node n = codes.item(i);
                    
                    if( n != null && n.hasChildNodes() ) {
                        error.code = Integer.parseInt(n.getFirstChild().getNodeValue().trim());
                    }
                }
                NodeList text = doc.getElementsByTagName("errortext");
                for( int i=0; i<text.getLength(); i++ ) {
                    Node n = text.item(i);
                    
                    if( n != null && n.hasChildNodes() ) {
                        error.message = n.getFirstChild().getNodeValue();
                    }
                }
            }
            catch( Throwable ignore ) {
                logger.warn("parseError(): Error was unparsable: " + ignore.getMessage());
                if( error.message == null ) {
                    error.message = assumedXml;
                }
            }
            if( error.message == null ) {
                if( httpStatus == 401 ) {
                    error.message = "Unauthorized user";
                }
                else if( httpStatus == 430 ) {
                    error.message = "Malformed parameters";
                }
                else if( httpStatus == 547 || httpStatus == 530 ) {
                    error.message = "Server error in cloud (" + httpStatus + ")";
                }
                else if( httpStatus == 531 ) {
                    error.message = "Unable to find account";
                }
                else {
                    error.message = "Received error code from server: " + httpStatus;
                }
            }
            return error;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudstackMethod.class.getName() + ".parseError()");
            }
        }
    }
    
    private Document parseResponse(int code, String xml) throws CloudException, InternalException {
        Logger wire = CloudstackProvider.getLogger(CloudstackMethod.class, "wire");
        Logger logger = CloudstackProvider.getLogger(CloudstackMethod.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudstackMethod.class.getName() + ".parseResponse(" + xml + ")");
        }
        try {
            try {
                if( wire.isDebugEnabled() ) {
                    wire.debug(xml);
                }
                ByteArrayInputStream input = new ByteArrayInputStream(xml.getBytes("utf-8"));
                
                return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(input);
            }
            catch( IOException e ) {
                throw new CloudException(e);
            }
            catch( ParserConfigurationException e ) {
                throw new CloudException(e);
            }
            catch( SAXException e ) {
                throw new CloudException("Received error code from server [" + code + "]: " + xml);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudstackMethod.class.getName() + ".parseResponse()");
            }
        }
    }
}
