package com.hp.octane.integrations.services.bridge;

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.SDKServiceBase;
import com.hp.octane.integrations.api.RestClient;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.configuration.OctaneConfiguration;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.connectivity.OctaneResultAbridged;
import com.hp.octane.integrations.dto.connectivity.OctaneTaskAbridged;
import com.hp.octane.integrations.dto.general.CIServerInfo;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by gullery on 05/08/2015.
 * <p/>
 * Bridge Service meant to provide an abridged connectivity functionality
 */

public final class BridgeService extends SDKServiceBase {
	private static final Logger logger = LogManager.getLogger(BridgeService.class);
	private static final DTOFactory dtoFactory = DTOFactory.getInstance();
	private ExecutorService connectivityExecutors = Executors.newFixedThreadPool(5, new AbridgedConnectivityExecutorsFactory());
	private ExecutorService taskProcessingExecutors = Executors.newFixedThreadPool(30, new AbridgedTasksExecutorsFactory());
	volatile private boolean shuttingDown = false;

	public BridgeService(Object configurator, boolean initBridge) {
		super(configurator);
		if (initBridge) {
			connect();
		}
	}

	private void connect() {
		if (!shuttingDown) {
			connectivityExecutors.execute(new Runnable() {
				public void run() {
					String tasksJSON;
					CIServerInfo serverInfo = getPluginServices().getServerInfo();
					try {
						tasksJSON = getAbridgedTasks(
								serverInfo.getInstanceId(),
								serverInfo.getType().value(),
								serverInfo.getUrl(),
								OctaneSDK.API_VERSION,
								OctaneSDK.SDK_VERSION);
						connect();
						if (tasksJSON != null && !tasksJSON.isEmpty()) {
							handleTasks(tasksJSON);
						}
					} catch (Exception e) {
						logger.error("connection to MQM Server temporary failed", e);
						doBreakableWait(1000);
						connect();
					}
				}
			});
		} else if (shuttingDown) {
			logger.info("bridge client stopped");
		}
	}

	private String getAbridgedTasks(String selfIdentity, String selfType, String selfLocation, Integer apiVersion, String sdkVersion) {
		String responseBody = null;
		RestClient restClientImpl = getRestService().obtainClient();
		OctaneConfiguration octaneConfiguration = getPluginServices().getOctaneConfiguration();
		if (octaneConfiguration != null && octaneConfiguration.isValid()) {
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("accept", "application/json");
			OctaneRequest octaneRequest = dtoFactory.newDTO(OctaneRequest.class)
					.setMethod(HttpMethod.GET)
					.setUrl(octaneConfiguration.getUrl() + "/internal-api/shared_spaces/" +
							octaneConfiguration.getSharedSpace() + "/analytics/ci/servers/" +
							selfIdentity + "/tasks?self-type=" + selfType + "&self-url=" + selfLocation + "&api-version=" + apiVersion + "&sdk-version=" + sdkVersion)
					.setHeaders(headers);
			try {
				OctaneResponse octaneResponse = restClientImpl.execute(octaneRequest);
				if (octaneResponse.getStatus() == HttpStatus.SC_OK) {
					responseBody = octaneResponse.getBody();
				} else {
					if (octaneResponse.getStatus() == HttpStatus.SC_REQUEST_TIMEOUT) {
						logger.info("expected timeout disconnection on retrieval of abridged tasks");
					} else if (octaneResponse.getStatus() == HttpStatus.SC_UNAUTHORIZED) {
						logger.error("connection to NGA Server failed: authentication error");
						doBreakableWait(5000);
					} else if (octaneResponse.getStatus() == HttpStatus.SC_FORBIDDEN) {
						logger.error("connection to NGA Server failed: authorization error");
						doBreakableWait(5000);
					} else if (octaneResponse.getStatus() == HttpStatus.SC_NOT_FOUND) {
						logger.error("connection to NGA Server failed: 404, API changes? version problem?");
						doBreakableWait(20000);
					} else {
						logger.info("unexpected response; status: " + octaneResponse.getStatus() + "; content: " + octaneResponse.getBody());
						doBreakableWait(2000);
					}
				}
			} catch (Exception e) {
				logger.error("failed to retrieve abridged tasks", e);
				doBreakableWait(2000);
			}
			return responseBody;
		} else {
			logger.info("NGA is not configured on this plugin, breathing before next retry");
			doBreakableWait(5000);
			return null;
		}
	}

	void dispose() {
		//  TODO: disconnect current connection once async connectivity is possible
		shuttingDown = true;
	}

	private void handleTasks(String tasksJSON) {
		try {
			OctaneTaskAbridged[] tasks = dtoFactory.dtoCollectionFromJson(tasksJSON, OctaneTaskAbridged[].class);
			logger.info("going to process " + tasks.length + " tasks");
			for (final OctaneTaskAbridged task : tasks) {
				taskProcessingExecutors.execute(new Runnable() {
					public void run() {
						OctaneResultAbridged result = getTasksProcessor().execute(task);
						int submitStatus = putAbridgedResult(
								getPluginServices().getServerInfo().getInstanceId(),
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
		RestClient restClientImpl = getRestService().obtainClient();
		OctaneConfiguration octaneConfiguration = getPluginServices().getOctaneConfiguration();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("content-type", "application/json");
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

	//  TODO: turn it to breakable wait with notifier
	private void doBreakableWait(long period) {
		try {
			Thread.sleep(period);
		} catch (InterruptedException ie) {
			logger.warn("interrupted while doing breakable wait");
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
