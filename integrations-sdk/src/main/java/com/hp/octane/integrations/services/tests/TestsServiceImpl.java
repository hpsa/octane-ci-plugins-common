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
 *
 */

package com.hp.octane.integrations.services.tests;

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.services.rest.RestClient;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.tests.TestsResult;
import com.hp.octane.integrations.services.queue.QueueService;
import com.hp.octane.integrations.spi.CIPluginServices;
import com.hp.octane.integrations.utils.CIPluginSDKUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Default implementation of tests service
 */

final class TestsServiceImpl implements TestsService {
	private static final Logger logger = LogManager.getLogger(TestsServiceImpl.class);
	private static final DTOFactory dtoFactory = DTOFactory.getInstance();
//	private static final String TESTS_QUEUE_FILE = "tests-queue.dat";

	//private final ObjectQueue<TestsResultQueueEntry> testsQueue;
	private final CIPluginServices pluginServices;
	private final RestService restService;

	private List<TestsResultQueueEntry> buildList = Collections.synchronizedList(new LinkedList<>());
	private int SERVICE_UNAVAILABLE_BREATHE_INTERVAL = 10000;
	private int LIST_EMPTY_INTERVAL = 3000;

	TestsServiceImpl(OctaneSDK.SDKServicesConfigurer configurer, QueueService queueService, RestService restService) {
		if (configurer == null || configurer.pluginServices == null) {
			throw new IllegalArgumentException("invalid configurer");
		}
		if (queueService == null) {
			throw new IllegalArgumentException("queue service MUST NOT be null");
		}
		if (restService == null) {
			throw new IllegalArgumentException("rest service MUST NOT be null");
		}

//		if (queueService.isPersistenceEnabled()) {
//			testsQueue = queueService.initFileQueue(TESTS_QUEUE_FILE, TestsResultQueueEntry.class);
//		} else {
//			testsQueue = queueService.initMemoQueue();
//		}

		this.pluginServices = configurer.pluginServices;
		this.restService = restService;

		logger.info("starting background worker...");
		Executors.newSingleThreadExecutor(new TestsResultPushWorkerThreadFactory())
				.execute(this::worker);
		logger.info("initialized SUCCESSFULLY");
	}

	@Override
	public boolean isTestsResultRelevant(String serverCiId, String jobCiId) throws IOException {
		if (serverCiId == null || serverCiId.isEmpty()) {
			throw new IllegalArgumentException("server CI ID MUST NOT be null nor empty");
		}
		if (jobCiId == null || jobCiId.isEmpty()) {
			throw new IllegalArgumentException("job CI ID MUST NOT be null nor empty");
		}

		OctaneRequest preflightRequest = dtoFactory.newDTO(OctaneRequest.class)
				.setMethod(HttpMethod.GET)
				.setUrl(getAnalyticsContextPath(pluginServices.getOctaneConfiguration().getUrl(), pluginServices.getOctaneConfiguration().getSharedSpace()) +
						"servers/" + CIPluginSDKUtils.urlEncodePathParam(serverCiId) +
						"/jobs/" + CIPluginSDKUtils.urlEncodePathParam(jobCiId) + "/tests-result-preflight");

		OctaneResponse response = restService.obtainOctaneRestClient().execute(preflightRequest);
		return response.getStatus() == HttpStatus.SC_OK && String.valueOf(true).equals(response.getBody());
	}

	public OctaneResponse pushTestsResult(TestsResult testsResult) throws IOException {
		if (testsResult == null) {
			throw new IllegalArgumentException("tests result MUST NOT be null");
		}

		String testsResultAsXml = dtoFactory.dtoToXml(testsResult);
		InputStream testsResultAsStream = new ByteArrayInputStream(testsResultAsXml.getBytes());
		return pushTestsResult(testsResultAsStream);
	}

	@Override
	public OctaneResponse pushTestsResult(InputStream testsResult) throws IOException {
		if (testsResult == null) {
			throw new IllegalArgumentException("tests result MUST NOT be null");
		}

		RestClient restClient = restService.obtainOctaneRestClient();
		Map<String, String> headers = new HashMap<>();
		headers.put(RestService.CONTENT_TYPE_HEADER, ContentType.APPLICATION_XML.getMimeType());
		OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
				.setMethod(HttpMethod.POST)
				.setUrl(getAnalyticsContextPath(pluginServices.getOctaneConfiguration().getUrl(), pluginServices.getOctaneConfiguration().getSharedSpace()) +
						"test-results?skip-errors=false")
				.setHeaders(headers)
				.setBody(testsResult);
		OctaneResponse response = restClient.execute(request);
		logger.info("tests result pushed; status: " + response.getStatus() + ", response: " + response.getBody());
		return response;
	}

	@Override
	public void enqueuePushTestsResult(String jobCiId, String buildCiId) {
		buildList.add(new TestsResultQueueEntry(jobCiId, buildCiId));
	}

	//  TODO: implement retries counter per item and strategy of discard
	//  TODO: distinct between the item's problem, server problem and env problem and retry strategy accordingly
	//  infallible everlasting background worker
	private void worker() {
		while (true) {
			if (!buildList.isEmpty()) {
				try {
					TestsResultQueueEntry testsResultQueueEntry = buildList.get(0);
					TestsResult testsResult = pluginServices.getTestsResult(testsResultQueueEntry.jobId, testsResultQueueEntry.buildId);
					OctaneResponse response = pushTestsResult(testsResult);
					if (response.getStatus() == HttpStatus.SC_ACCEPTED) {
						logger.info("tests result push SUCCEED");
						buildList.remove(0);
					} else if (response.getStatus() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
						logger.info("tests result push FAILED, service unavailable; retrying after a breathe...");
						breathe(SERVICE_UNAVAILABLE_BREATHE_INTERVAL);
					} else {
						//  case of any other fatal error
						logger.error("tests result push FAILED, status " + response.getStatus() + "; dropping this item from the queue");
						buildList.remove(0);
					}

				} catch (IOException e) {
					logger.error("tests result push failed; will retry after " + SERVICE_UNAVAILABLE_BREATHE_INTERVAL + "ms", e);
					breathe(SERVICE_UNAVAILABLE_BREATHE_INTERVAL);
				} catch (Throwable t) {
					logger.error("tests result push failed; dropping this item from the queue ", t);
					buildList.remove(0);
				}
			} else {
				breathe(LIST_EMPTY_INTERVAL);
			}
		}
	}

	//  TODO: turn to be breakable wait with timeout and notifier
	private void breathe(int period) {
		try {
			Thread.sleep(period);
		} catch (InterruptedException ie) {
			logger.error("interrupted while breathing", ie);
		}
	}

	private String getAnalyticsContextPath(String octaneBaseUrl, String sharedSpaceId) {
		return octaneBaseUrl + RestService.SHARED_SPACE_INTERNAL_API_PATH_PART + sharedSpaceId + RestService.ANALYTICS_CI_PATH_PART;
	}

	private static final class TestsResultQueueEntry implements QueueService.QueueItem {
		private String jobId;
		private String buildId;

		//  [YG] this constructor MUST be present
		private TestsResultQueueEntry() {
		}

		private TestsResultQueueEntry(String jobId, String buildId) {
			this.jobId = jobId;
			this.buildId = buildId;
		}

		@Override
		public String toString() {
			return "'" + jobId + " #" + buildId + "'";
		}
	}

	private static final class TestsResultPushWorkerThreadFactory implements ThreadFactory {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("TestsResultPushWorker-" + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}
