/*
 *     Copyright 2017 Hewlett-Packard Development Company, L.P.
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
 *
 */

package com.hp.octane.integrations.services.bridge;

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.api.RestClient;
import com.hp.octane.integrations.api.RestService;
import com.hp.octane.integrations.api.TasksProcessor;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.configuration.OctaneConfiguration;
import com.hp.octane.integrations.dto.connectivity.*;
import com.hp.octane.integrations.dto.general.CIPluginInfo;
import com.hp.octane.integrations.dto.general.CIServerInfo;
import com.hp.octane.integrations.spi.CIPluginServices;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.hp.octane.integrations.util.CIPluginUtils.doWait;

/**
 * Bridge Service meant to provide an abridged connection functionality
 */

public final class BridgeServiceImpl extends OctaneSDK.SDKServiceBase {
	private static final Logger logger = LogManager.getLogger(BridgeServiceImpl.class);
	private static final DTOFactory dtoFactory = DTOFactory.getInstance();
	private static final String ACCEPT_HEADER = "accept";
	private static final String CONTENT_TYPE_HEADER = "content-type";
	private static final String APPLICATION_JSON_MIME = "application/json";

	private ExecutorService connectivityExecutors = Executors.newFixedThreadPool(5, new AbridgedConnectivityExecutorsFactory());
	private ExecutorService taskProcessingExecutors = Executors.newFixedThreadPool(30, new AbridgedTasksExecutorsFactory());

	private final CIPluginServices pluginServices;
	private final RestService restService;
	private final TasksProcessor tasksProcessor;

	public BridgeServiceImpl(Object configurator, CIPluginServices pluginServices, RestService restService, TasksProcessor tasksProcessor) {
		super(configurator);

		if (pluginServices == null) {
			throw new IllegalArgumentException("plugin services MUST NOT be null");
		}
		if (restService == null) {
			throw new IllegalArgumentException("rest service MUST NOT be null");
		}
		if (tasksProcessor == null) {
			throw new IllegalArgumentException("task processor MUST NOT be null");
		}

		this.pluginServices = pluginServices;
		this.restService = restService;
		this.tasksProcessor = tasksProcessor;
		connect();
	}

	private void connect() {
		if (!connectivityExecutors.isShutdown()) {
			connectivityExecutors.execute(new Runnable() {
				public void run() {
					String tasksJSON;
					CIServerInfo serverInfo = pluginServices.getServerInfo();
					CIPluginInfo pluginInfo = pluginServices.getPluginInfo();
					String apiKey = pluginServices.getOctaneConfiguration() == null ? "" : pluginServices.getOctaneConfiguration().getApiKey();

					try {
						//  get tasks, wait if needed and return with task or timeout or error
						tasksJSON = getAbridgedTasks(
								serverInfo.getInstanceId(),
								serverInfo.getType(),
								serverInfo.getUrl(),
								OctaneSDK.API_VERSION,
								OctaneSDK.SDK_VERSION,
								pluginInfo == null ? "" : pluginInfo.getVersion(),
								apiKey,
								serverInfo.getImpersonatedUser() == null ? "" : serverInfo.getImpersonatedUser());

						//  regardless of response - reconnect again to keep the light on
						connect();

						//  now can process the received tasks - if any
						if (tasksJSON != null && !tasksJSON.isEmpty()) {
							handleTasks(tasksJSON);
						}
					} catch (Exception e) {
						logger.error("connection to Octane Server temporary failed", e);
						doWait(1000);
						connect();
					}
				}
			});
		} else {
			logger.info("bridge service stopped gracefully by external request");
		}
	}

	private String getAbridgedTasks(String selfIdentity, String selfType, String selfUrl, Integer apiVersion, String sdkVersion, String pluginVersion, String octaneUser, String ciServerUser) {
		//  pre-process potentially non-URL-safe values
		String selfUrlEscaped = selfUrl;
		try {
			selfUrlEscaped = URLEncoder.encode(selfUrl, StandardCharsets.UTF_8.name());
		} catch (UnsupportedEncodingException uee) {
			logger.warn("failed to URL-encode server URL '" + selfUrl + "' (will be sent as is", uee);
		}
		String sdkVersionEscaped = sdkVersion;
		try {
			sdkVersionEscaped = URLEncoder.encode(sdkVersion, StandardCharsets.UTF_8.name());
		} catch (UnsupportedEncodingException uee) {
			logger.warn("failed to URL-encode SDK version '" + selfUrl + "' (will be sent as is", uee);
		}
		String pluginVersionEscaped = pluginVersion;
		try {
			pluginVersionEscaped = URLEncoder.encode(pluginVersion, StandardCharsets.UTF_8.name());
		} catch (UnsupportedEncodingException uee) {
			logger.warn("failed to URL-encode plugin version '" + selfUrl + "' (will be sent as is", uee);
		}

		String responseBody = null;
		RestClient restClient = restService.obtainClient();
		OctaneConfiguration octaneConfiguration = pluginServices.getOctaneConfiguration();
		if (octaneConfiguration != null && octaneConfiguration.isValid()) {
			Map<String, String> headers = new HashMap<>();
			headers.put(ACCEPT_HEADER, APPLICATION_JSON_MIME);
			OctaneRequest octaneRequest = dtoFactory.newDTO(OctaneRequest.class)
					.setMethod(HttpMethod.GET)
					.setUrl(octaneConfiguration.getUrl() + "/internal-api/shared_spaces/" + octaneConfiguration.getSharedSpace() + "/analytics/ci/servers/" + selfIdentity +
							"/tasks?self-type=" + selfType + "&self-url=" + selfUrlEscaped + "&api-version=" + apiVersion + "&sdk-version=" + sdkVersionEscaped +
							"&plugin-version=" + pluginVersionEscaped + "&client-id=" + octaneUser + "&ci-server-user=" + ciServerUser)
					.setHeaders(headers);
			try {
				OctaneResponse octaneResponse = restClient.execute(octaneRequest);
				if (octaneResponse.getStatus() == HttpStatus.SC_OK) {
					responseBody = octaneResponse.getBody();
				} else {
					if (octaneResponse.getStatus() == HttpStatus.SC_REQUEST_TIMEOUT) {
						logger.debug("expected timeout disconnection on retrieval of abridged tasks");
					} else if (octaneResponse.getStatus() == HttpStatus.SC_UNAUTHORIZED) {
						logger.error("connection to Octane failed: authentication error");
						doWait(5000);
					} else if (octaneResponse.getStatus() == HttpStatus.SC_FORBIDDEN) {
						logger.error("connection to Octane failed: authorization error");
						doWait(5000);
					} else if (octaneResponse.getStatus() == HttpStatus.SC_NOT_FOUND) {
						logger.error("connection to Octane failed: 404, API changes? version problem?");
						doWait(20000);
					} else {
						logger.error("unexpected response from Octane; status: " + octaneResponse.getStatus() + ", content: " + octaneResponse.getBody());
						doWait(2000);
					}
				}
			} catch (Exception e) {
				logger.error("failed to retrieve abridged tasks", e);
				doWait(2000);
			}
			return responseBody;
		} else {
			logger.info("Octane is not configured on this plugin or the configuration is not valid, breathing before next retry");
			doWait(5000);
			return null;
		}
	}

	private void handleTasks(String tasksJSON) {
		try {
			OctaneTaskAbridged[] tasks = dtoFactory.dtoCollectionFromJson(tasksJSON, OctaneTaskAbridged[].class);
			logger.info("going to process " + tasks.length + " tasks");
			for (final OctaneTaskAbridged task : tasks) {
				taskProcessingExecutors.execute(new Runnable() {
					public void run() {
						OctaneResultAbridged result = tasksProcessor.execute(task);
						int submitStatus = putAbridgedResult(
								pluginServices.getServerInfo().getInstanceId(),
								result.getId(),
								dtoFactory.dtoToJson(result));
						logger.info("result for task '" + result.getId() + "' submitted with status " + submitStatus);
					}
				});
			}
		} catch (Exception e) {
			logger.error("failed to process tasks", e);
		}
	}

	private int putAbridgedResult(String selfIdentity, String taskId, String contentJSON) {
		RestClient restClientImpl = restService.obtainClient();
		OctaneConfiguration octaneConfiguration = pluginServices.getOctaneConfiguration();
		Map<String, String> headers = new LinkedHashMap<>();
		headers.put(CONTENT_TYPE_HEADER, APPLICATION_JSON_MIME);
		OctaneRequest octaneRequest = dtoFactory.newDTO(OctaneRequest.class)
				.setMethod(HttpMethod.PUT)
				.setUrl(octaneConfiguration.getUrl() + "/internal-api/shared_spaces/" + octaneConfiguration.getSharedSpace() + "/analytics/ci/servers/" + selfIdentity + "/tasks/" + taskId + "/result")
				.setHeaders(headers)
				.setBody(contentJSON);
		try {
			OctaneResponse octaneResponse = restClientImpl.execute(octaneRequest);
			return octaneResponse.getStatus();
		} catch (IOException ioe) {
			logger.error("failed to submit abridged task's result", ioe);
			return 0;
		}
	}

	private static final class AbridgedConnectivityExecutorsFactory implements ThreadFactory {
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("AbridgedConnectivityThread-" + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	private static final class AbridgedTasksExecutorsFactory implements ThreadFactory {
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("AbridgedTasksExecutorsFactory-" + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}
