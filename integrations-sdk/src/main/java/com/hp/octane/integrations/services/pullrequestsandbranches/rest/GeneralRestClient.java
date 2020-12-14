/*
 *     Copyright 2017 EntIT Software LLC, a Micro Focus company, L.P.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.hp.octane.integrations.services.pullrequestsandbranches.rest;


import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.configuration.CIProxyConfiguration;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;
import com.hp.octane.integrations.utils.CIPluginSDKUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class GeneralRestClient {

    private final String CONTENT_ENCODING_HEADER = "content-encoding";
    private final String GZIP_ENCODING = "gzip";

    private static final Logger logger = LogManager.getLogger(GeneralRestClient.class);
    private final CloseableHttpClient httpClient;
    private final AuthenticationStrategy authentication;

    public GeneralRestClient(AuthenticationStrategy authentication) {
        this.authentication = authentication;
        this.httpClient = HttpClients.createDefault();
    }

    public OctaneResponse executeRequest(OctaneRequest request) throws IOException {
        if (authentication.isAuthenticationNeeded()) {
            authentication.authenticate(httpClient, false);
        }
        OctaneResponse result;
        HttpClientContext context;

        HttpResponse httpResponse = null;

        try {
            //  we are running this loop either once or twice: once - regular flow, twice - when retrying after re-login attempt
            for (int i = 0; i < 2; i++) {
                HttpUriRequest uriRequest = createHttpRequest(request);
                context = createHttpContext(request.getUrl(), request.getTimeoutSec());
                httpResponse = httpClient.execute(uriRequest, context);

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                if (i == 0 && HttpStatus.SC_UNAUTHORIZED == statusCode && authentication.supportAuthenticationOnUnauthorizedException()) {
                    EntityUtils.consumeQuietly(httpResponse.getEntity());
                    HttpClientUtils.closeQuietly(httpResponse);
                    if (!authentication.authenticate(httpClient, true)) {
                        break;
                    }
                } else {
                    authentication.onResponse(httpResponse, context);
                    break;
                }
            }

            result = createResponse(httpResponse);
        } finally {
            if (httpResponse != null) {
                EntityUtils.consumeQuietly(httpResponse.getEntity());
                HttpClientUtils.closeQuietly(httpResponse);
            }
        }

        return result;
    }

    private OctaneResponse createResponse(HttpResponse response) throws IOException {
        OctaneResponse octaneResponse = DTOFactory.getInstance().newDTO(OctaneResponse.class)
                .setStatus(response.getStatusLine().getStatusCode());

        //set body
        if (response.getEntity() != null) {
            octaneResponse.setBody(CIPluginSDKUtils.inputStreamToUTF8String(response.getEntity().getContent()));
        }

        //set headers
        if (response.getAllHeaders() != null && response.getAllHeaders().length > 0) {
            Map<String, String> mapHeaders = new HashMap<>();
            for (Header header : response.getAllHeaders()) {
                mapHeaders.put(header.getName(), header.getValue());
            }
            octaneResponse.setHeaders(mapHeaders);
        }

        return octaneResponse;
    }

    /**
     * This method should be the ONLY mean that creates Http Request objects
     *
     * @param octaneRequest Request data as it is maintained in Octane related flavor
     * @return pre-configured HttpUriRequest
     */
    private HttpUriRequest createHttpRequest(OctaneRequest octaneRequest) {
        HttpUriRequest request;
        RequestBuilder requestBuilder;

        //  create base request by METHOD
        if (octaneRequest.getMethod().equals(HttpMethod.GET)) {
            requestBuilder = RequestBuilder.get(octaneRequest.getUrl());
        } else if (octaneRequest.getMethod().equals(HttpMethod.DELETE)) {
            requestBuilder = RequestBuilder.delete(octaneRequest.getUrl());
        } else if (octaneRequest.getMethod().equals(HttpMethod.POST)) {
            requestBuilder = RequestBuilder.post(octaneRequest.getUrl());
            requestBuilder.addHeader(new BasicHeader(CONTENT_ENCODING_HEADER, GZIP_ENCODING));
            requestBuilder.setEntity(new GzipCompressingEntity(new InputStreamEntity(octaneRequest.getBody(), ContentType.APPLICATION_JSON)));
        } else if (octaneRequest.getMethod().equals(HttpMethod.PUT)) {
            requestBuilder = RequestBuilder.put(octaneRequest.getUrl());
            requestBuilder.addHeader(new BasicHeader(CONTENT_ENCODING_HEADER, GZIP_ENCODING));
            requestBuilder.setEntity(new GzipCompressingEntity(new InputStreamEntity(octaneRequest.getBody(), ContentType.APPLICATION_JSON)));
        } else {
            throw new RuntimeException("HTTP method " + octaneRequest.getMethod() + " not supported");
        }

        //  set custom headers
        if (octaneRequest.getHeaders() != null) {
            for (Map.Entry<String, String> e : octaneRequest.getHeaders().entrySet()) {
                requestBuilder.setHeader(e.getKey(), e.getValue());
            }
        }

        authentication.onCreateHttpRequest(requestBuilder);
        request = requestBuilder.build();
        return request;
    }

    private HttpClientContext createHttpContext(String requestUrl, int requestTimeoutSec) {
        HttpClientContext context = HttpClientContext.create();
        context.setCookieStore(new BasicCookieStore());


        //  prepare request config
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD);

        //  configure proxy if needed
        CIProxyConfiguration proxyConfiguration = CIPluginSDKUtils.getProxyConfiguration(requestUrl, null);
        if (proxyConfiguration != null) {
            HttpHost proxyHost = new HttpHost(proxyConfiguration.getHost(), proxyConfiguration.getPort());

            if (proxyConfiguration.getUsername() != null && !proxyConfiguration.getUsername().isEmpty()) {
                AuthScope authScope = new AuthScope(proxyHost);
                Credentials credentials = new UsernamePasswordCredentials(proxyConfiguration.getUsername(), proxyConfiguration.getPassword());
                CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
                credentialsProvider.setCredentials(authScope, credentials);
                context.setCredentialsProvider(credentialsProvider);
            }
            requestConfigBuilder.setProxy(proxyHost);
        }

        // set timeout if needed
        if (requestTimeoutSec > 0) {
            int timeoutMs = requestTimeoutSec * 1000;
            requestConfigBuilder
                    .setConnectTimeout(timeoutMs)
                    .setConnectionRequestTimeout(timeoutMs)
                    .setSocketTimeout(timeoutMs);
        }


        context.setRequestConfig(requestConfigBuilder.build());
        authentication.onCreateContext(context);
        return context;
    }

    public void shutdown() {
        HttpClientUtils.closeQuietly(httpClient);
    }
}
