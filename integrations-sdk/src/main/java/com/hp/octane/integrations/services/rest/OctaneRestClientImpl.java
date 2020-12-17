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

package com.hp.octane.integrations.services.rest;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.hp.octane.integrations.OctaneConfiguration;
import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.configuration.CIProxyConfiguration;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
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
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * REST Client default implementation
 */

final class OctaneRestClientImpl implements OctaneRestClient {
	private static final Logger logger = LogManager.getLogger(OctaneRestClientImpl.class);
	private static final DTOFactory dtoFactory = DTOFactory.getInstance();

	private static final Set<Integer> AUTHENTICATION_ERROR_CODES = Stream.of(HttpStatus.SC_UNAUTHORIZED).collect(Collectors.toSet());
	private static final String LWSSO_COOKIE_NAME = "LWSSO_COOKIE_KEY";
	private static final String AUTHENTICATION_URI = "authentication/sign_in";
	private static final int MAX_TOTAL_CONNECTIONS = 20;

	private final OctaneSDK.SDKServicesConfigurer configurer;
	private final CloseableHttpClient httpClient;

	private final ExecutorService requestMonitorExecutors = Executors.newFixedThreadPool(5, new RequestMonitorExecutorsFactory());
	private long requestMonitorExecutorsAbortedCount = 0;
	private long lastRequestMonitorWorkerTime = 0;
	private final Map<HttpUriRequest, Long> ongoingRequests2Started = new HashMap();
	private final long REQUEST_ABORT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(120);//120 sec in ms
	private final Object REQUESTS_LIST_LOCK = new Object();
	private final Object RESET_LWSSO_TOKEN_LOCK = new Object();
	private boolean shutdownActivated = false;

	private Cookie LWSSO_TOKEN = null;
	private long loginRequiredForRefreshLwssoTokenUntil = 0;

	OctaneRestClientImpl(OctaneSDK.SDKServicesConfigurer configurer) {
		if (configurer == null) {
			throw new IllegalArgumentException("invalid configurer");
		}

		requestMonitorExecutors.execute(this::requestMonitorWorker);

		this.configurer = configurer;

		SSLContext sslContext;
		try {
			sslContext = SSLContext.getInstance("SSL");
			sslContext.init(null, getTrustManagers(), new java.security.SecureRandom());
		} catch (Exception e) {
			logger.warn(configurer.octaneConfiguration.geLocationForLog() + "Failed to create sslContext with customTrustManagers. Using systemDefault sslContext. Error : " + e.getMessage());
			sslContext = SSLContexts.createSystemDefault();
		}

		HostnameVerifier hostnameVerifier = new CustomHostnameVerifier();
		SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
		Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
				.register("http", PlainConnectionSocketFactory.getSocketFactory())
				.register("https", sslSocketFactory)
				.build();
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
		connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
		connectionManager.setDefaultMaxPerRoute(MAX_TOTAL_CONNECTIONS);

		HttpClientBuilder clientBuilder = HttpClients.custom()
				.setConnectionManager(connectionManager);

		httpClient = clientBuilder.build();
	}

	@Override
	public OctaneResponse execute(OctaneRequest request) throws IOException {
		return executeRequest(request, configurer.octaneConfiguration);
	}

	@Override
	public OctaneResponse execute(OctaneRequest request, OctaneConfiguration configuration) throws IOException {
		return executeRequest(request, configuration);
	}

	@Override
	public void shutdown() {
		shutdownActivated = true;
		logger.info(configurer.octaneConfiguration.geLocationForLog() + "starting REST client shutdown sequence...");
		abortAllRequests();
		logger.info(configurer.octaneConfiguration.geLocationForLog() + "closing the client...");
		HttpClientUtils.closeQuietly(httpClient);
		requestMonitorExecutors.shutdown();
		logger.info(configurer.octaneConfiguration.geLocationForLog() + "REST client shutdown done");
	}

	void notifyConfigurationChange() {
		synchronized (RESET_LWSSO_TOKEN_LOCK) {
			loginRequiredForRefreshLwssoTokenUntil = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
			LWSSO_TOKEN = null;
		}
	}

	private void abortAllRequests() {
		logger.info(configurer.octaneConfiguration.geLocationForLog() + "aborting " + ongoingRequests2Started.size() + " request/s...");
		synchronized (REQUESTS_LIST_LOCK) {
			for (HttpUriRequest request : ongoingRequests2Started.keySet()) {
				logger.info(configurer.octaneConfiguration.geLocationForLog() + "\taborting " + request);
				request.abort();
			}
			LWSSO_TOKEN = null;
		}
	}

	private OctaneResponse executeRequest(OctaneRequest request, OctaneConfiguration configuration) throws IOException {
		OctaneResponse result;
		HttpClientContext context;
		HttpUriRequest uriRequest = null;
		HttpResponse httpResponse = null;
		OctaneResponse loginResponse;
		if (LWSSO_TOKEN == null) {
			logger.info(configurer.octaneConfiguration.geLocationForLog() + "initial login");
			loginResponse = login(configuration);
			if (loginResponse.getStatus() != 200) {
				logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed on initial login, status " + loginResponse.getStatus());
				return loginResponse;
			}
		}

		try {
			//  we are running this loop either once or twice: once - regular flow, twice - when retrying after re-login attempt
			for (int i = 0; i < 2; i++) {
				uriRequest = createHttpRequest(request);
				context = createHttpContext(request.getUrl(), request.getTimeoutSec(), false);
				synchronized (REQUESTS_LIST_LOCK) {
					ongoingRequests2Started.put(uriRequest, System.currentTimeMillis());
				}
				httpResponse = httpClient.execute(uriRequest, context);
				synchronized (REQUESTS_LIST_LOCK) {
					ongoingRequests2Started.remove(uriRequest);
				}

				if (AUTHENTICATION_ERROR_CODES.contains(httpResponse.getStatusLine().getStatusCode())) {
					logger.info(configurer.octaneConfiguration.geLocationForLog() + "doing RE-LOGIN due to status " + httpResponse.getStatusLine().getStatusCode() + " received while calling " + request.getUrl());
					EntityUtils.consumeQuietly(httpResponse.getEntity());
					HttpClientUtils.closeQuietly(httpResponse);
					loginResponse = login(configuration);
					if (loginResponse.getStatus() != 200) {
						logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed to RE-LOGIN with status " + loginResponse.getStatus() + ", won't attempt the original request anymore");
						return loginResponse;
					} else {
						logger.info(configurer.octaneConfiguration.geLocationForLog() + "re-attempting the original request (" + request.getUrl() + ") having successful RE-LOGIN");
					}
				} else {
					refreshSecurityToken(context, false);
					break;
				}
			}

			result = createNGAResponse(request, httpResponse);
		} catch (IOException ioe) {
			logger.debug(configurer.octaneConfiguration.geLocationForLog() + "failed executing " + request, ioe);
			throw ioe;
		} finally {
			if (uriRequest != null && ongoingRequests2Started.containsKey(uriRequest)) {
				synchronized (REQUESTS_LIST_LOCK) {
					ongoingRequests2Started.remove(uriRequest);
				}
			}
			if (httpResponse != null) {
				EntityUtils.consumeQuietly(httpResponse.getEntity());
				HttpClientUtils.closeQuietly(httpResponse);
			}
		}

		return result;
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
			requestBuilder.addHeader(new BasicHeader(RestService.CONTENT_ENCODING_HEADER, RestService.GZIP_ENCODING));
			requestBuilder.setEntity(new GzipCompressingEntity(new InputStreamEntity(octaneRequest.getBody(), ContentType.APPLICATION_JSON)));
		} else if (octaneRequest.getMethod().equals(HttpMethod.PUT)) {
			requestBuilder = RequestBuilder.put(octaneRequest.getUrl());
			requestBuilder.addHeader(new BasicHeader(RestService.CONTENT_ENCODING_HEADER, RestService.GZIP_ENCODING));
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

		//  set system headers
		requestBuilder.setHeader(CLIENT_TYPE_HEADER, CLIENT_TYPE_VALUE);

		request = requestBuilder.build();
		return request;
	}

	private HttpClientContext createHttpContext(String requestUrl, int requestTimeoutSec, boolean isLoginRequest) {
		HttpClientContext context = HttpClientContext.create();
		context.setCookieStore(new BasicCookieStore());

		//  add security token if needed
		if (!isLoginRequest) {
			context.getCookieStore().addCookie(LWSSO_TOKEN);
		}

		//  prepare request config
		RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
				.setCookieSpec(CookieSpecs.STANDARD);

		//  configure proxy if needed
		CIProxyConfiguration proxyConfiguration = CIPluginSDKUtils.getProxyConfiguration(requestUrl, configurer);
		if (proxyConfiguration != null) {
			logger.debug(configurer.octaneConfiguration.geLocationForLog() + "proxy will be used with the following setup: " + proxyConfiguration);
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
		return context;
	}

	private void refreshSecurityToken(HttpClientContext context, boolean isLogin) {
		for (Cookie cookie : context.getCookieStore().getCookies()) {
			if (LWSSO_COOKIE_NAME.equals(cookie.getName()) && (LWSSO_TOKEN == null || cookie.getValue().compareTo(LWSSO_TOKEN.getValue()) != 0)) {
				((BasicClientCookie) cookie).setPath("/");

				synchronized (RESET_LWSSO_TOKEN_LOCK) {
					if (!isLogin && loginRequiredForRefreshLwssoTokenUntil > System.currentTimeMillis()) {
						logger.info(configurer.octaneConfiguration.geLocationForLog() + "refreshSecurityToken is cancelled");
					} else {
						LWSSO_TOKEN = cookie;
						logger.debug(configurer.octaneConfiguration.geLocationForLog() + "successfully refreshed security token.isLogin=" + isLogin);
					}
				}

				break;
			}
		}
	}

	private OctaneResponse createNGAResponse(OctaneRequest request, HttpResponse response) throws IOException {
		OctaneResponse octaneResponse = dtoFactory.newDTO(OctaneResponse.class)
				.setStatus(response.getStatusLine().getStatusCode());
		if (response.getEntity() != null) {
			octaneResponse.setBody(CIPluginSDKUtils.inputStreamToUTF8String(response.getEntity().getContent()));
		}
		if (response.getAllHeaders() != null && response.getAllHeaders().length > 0) {
			Map<String, String> mapHeaders = new HashMap<>();
			for (Header header : response.getAllHeaders()) {
				mapHeaders.put(header.getName(), header.getValue());
			}
			octaneResponse.setHeaders(mapHeaders);
		}
		if (request != null && request.getHeaders() != null) {
			octaneResponse.setCorrelationId(request.getHeaders().get(RestService.CORRELATION_ID_HEADER));
		}
		return octaneResponse;
	}

	private OctaneResponse login(OctaneConfiguration config) throws IOException {
		OctaneResponse result;
		HttpResponse response = null;

		try {
			HttpUriRequest loginRequest = buildLoginRequest(config);
			HttpClientContext context = createHttpContext(loginRequest.getURI().toString(), 0, true);
			response = httpClient.execute(loginRequest, context);

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				refreshSecurityToken(context, true);
			} else {
				logger.warn(configurer.octaneConfiguration.geLocationForLog() + "failed to login; response status: " + response.getStatusLine().getStatusCode());
			}
			result = createNGAResponse(null, response);
		} catch (IOException ioe) {
			logger.debug(configurer.octaneConfiguration.geLocationForLog() + "failed to login", ioe);
			throw ioe;
		} finally {
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
				HttpClientUtils.closeQuietly(response);
			}
		}

		return result;
	}

	private HttpUriRequest buildLoginRequest(OctaneConfiguration config) throws IOException {
		HttpUriRequest loginRequest;
		try {
			LoginApiBody loginApiBody = new LoginApiBody(config.getClient(), config.getSecret());
			StringEntity loginApiJson = new StringEntity(CIPluginSDKUtils.getObjectMapper().writeValueAsString(loginApiBody), ContentType.APPLICATION_JSON);
			RequestBuilder requestBuilder = RequestBuilder.post(config.getUrl() + "/" + AUTHENTICATION_URI)
					.setHeader(CLIENT_TYPE_HEADER, CLIENT_TYPE_VALUE)
					.setEntity(loginApiJson);
			loginRequest = requestBuilder.build();
			return loginRequest;
		} catch (JsonProcessingException jpe) {
			throw new IOException("failed to serialize login content", jpe);
		}
	}

	private TrustManager[] getTrustManagers() throws NoSuchAlgorithmException, KeyStoreException {
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init((KeyStore) null);
		TrustManager[] tmArr = tmf.getTrustManagers();
		if (tmArr.length == 1 && tmArr[0] instanceof X509TrustManager) {
			X509TrustManager defaultTm = (X509TrustManager) tmArr[0];
			TrustManager myTM = new X509TrustManager() {
				public X509Certificate[] getAcceptedIssuers() {
					return defaultTm.getAcceptedIssuers();
				}

				public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
					defaultTm.checkClientTrusted(certs, authType);
				}

				public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
					try {
						defaultTm.checkServerTrusted(certs, authType);
					} catch (CertificateException e) {
						for (X509Certificate cer : certs) {
							if (cer.getIssuerDN().getName() != null && cer.getIssuerDN().getName().toLowerCase().contains("microfocus")) {
								return;
							}
						}
						throw e;
					}
				}
			};

			return new TrustManager[]{myTM};
		} else {
			logger.info(configurer.octaneConfiguration.geLocationForLog() + "Using only default trust managers. Received " + tmArr.length + " trust managers."
					+ ((tmArr.length > 0) ? "First one is :" + tmArr[0].getClass().getCanonicalName() : ""));
			return tmArr;
		}
	}

	private void requestMonitorWorker() {
		while (!shutdownActivated) {
			lastRequestMonitorWorkerTime = System.currentTimeMillis();
			try {
				synchronized (REQUESTS_LIST_LOCK) {
					ongoingRequests2Started.entrySet().forEach(entry -> {
						long expectedEnd = entry.getValue() + REQUEST_ABORT_TIMEOUT_MS;
						long diff = System.currentTimeMillis() - expectedEnd;
						if (diff > 0 && !entry.getKey().isAborted()) {
							logger.info(configurer.octaneConfiguration.geLocationForLog() + " Aborting " + entry.getKey() + " as expected timeout is over ");
							entry.getKey().abort();
							requestMonitorExecutorsAbortedCount++;
						}
					});
				}
			} catch (Exception e) {
				logger.error(configurer.octaneConfiguration.geLocationForLog() + "requestMonitorWorker error : " + e.getMessage(), e);
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				logger.error(configurer.octaneConfiguration.geLocationForLog() + "requestMonitorWorker sleep interrupted : " + e.getMessage());
			}
		}
	}

	@Override
	public Map<String, Object> getMetrics() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("requestMonitorExecutorsAbortedCount", requestMonitorExecutorsAbortedCount);
		map.put("ongoingRequests.size", ongoingRequests2Started.size());
		map.put("lastRequestMonitorWorkerTime", new Date(lastRequestMonitorWorkerTime));
		return map;
	}

	public static final class CustomHostnameVerifier implements HostnameVerifier {
		private final HostnameVerifier defaultVerifier = new DefaultHostnameVerifier();

		public boolean verify(String host, SSLSession sslSession) {
			boolean result = defaultVerifier.verify(host, sslSession);
			if (!result) {
				try {
					Certificate[] ex = sslSession.getPeerCertificates();
					X509Certificate x509 = (X509Certificate) ex[0];
					Collection<List<?>> altNames = x509.getSubjectAlternativeNames();
					for (List<?> namePair : altNames) {
						if (namePair != null &&
								namePair.size() > 1 &&
								namePair.get(1) instanceof String &&
								"*.saas.microfocus.com".equals(namePair.get(1))) {
							result = true;
							break;
						}
					}
				} catch (CertificateParsingException cpe) {
					logger.error("failed to parse certificate", cpe);       //  result will remain false
				} catch (SSLException ssle) {
					logger.error("failed to handle certificate", ssle);     //  result will remain false
				}
			}
			return result;
		}
	}

	@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
	private static final class LoginApiBody {
		private final String client_id;
		private final String client_secret;

		private LoginApiBody(String client_id, String client_secret) {
			this.client_id = client_id;
			this.client_secret = client_secret;
		}

		@Override
		public String toString() {
			return "LoginApiBody {" +
					"client_id: " + client_id +
					", client_secret: " + client_secret + "}";
		}
	}

	private static final class RequestMonitorExecutorsFactory implements ThreadFactory {
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("OctaneRestClientRequestWorker-" + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}
