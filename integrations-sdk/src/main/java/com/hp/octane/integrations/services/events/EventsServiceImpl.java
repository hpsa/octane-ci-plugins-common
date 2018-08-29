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

package com.hp.octane.integrations.services.events;

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.api.EventsService;
import com.hp.octane.integrations.api.RestService;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.configuration.OctaneConfiguration;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.events.CIEvent;
import com.hp.octane.integrations.dto.events.CIEventsList;
import com.hp.octane.integrations.exceptions.PermanentException;
import com.hp.octane.integrations.exceptions.TemporaryException;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static com.hp.octane.integrations.api.RestService.ANALYTICS_CI_PATH_PART;
import static com.hp.octane.integrations.api.RestService.CONTENT_TYPE_HEADER;
import static com.hp.octane.integrations.api.RestService.SHARED_SPACE_INTERNAL_API_PATH_PART;

/**
 * EventsService implementation
 */

public final class EventsServiceImpl extends OctaneSDK.SDKServiceBase implements EventsService {
	private static final Logger logger = LogManager.getLogger(EventsServiceImpl.class);
	private static final DTOFactory dtoFactory = DTOFactory.getInstance();

	private final ExecutorService executorService = Executors.newSingleThreadExecutor(new EventsServiceWorkerThreadFactory());
	private final RestService restService;
	private final List<CIEvent> events;

	private final int EVENTS_CHUNK_SIZE = 10;
	private final int MAX_EVENTS_TO_KEEP = 3000;
	private final long NO_EVENTS_PAUSE = 5000;
	private final long OCTANE_CONFIGURATION_UNAVAILABLE_PAUSE = 20000;
	private final long TEMPORARY_FAILURE_PAUSE = 10000;

	public EventsServiceImpl(Object internalUsageValidator, RestService restService) {
		super(internalUsageValidator);

		if (restService == null) {
			throw new IllegalArgumentException("rest service MUST NOT be null");
		}

		this.restService = restService;
		this.events = new LinkedList<>();

		logger.info("starting background worker...");
		executorService.execute(this::worker);
		logger.info("initialized SUCCESSFULLY");
	}

	@Override
	public void publishEvent(CIEvent event) {
		synchronized (events) {
			events.add(event);
			if (events.size() > MAX_EVENTS_TO_KEEP) {
				logger.warn("reached MAX amount of events to keep in queue (max -" + MAX_EVENTS_TO_KEEP + ", found - " + events.size() + "), capping the head");
				while (events.size() > MAX_EVENTS_TO_KEEP) {
					events.remove(0);
				}
			}
		}
		//  TODO: release events available monitor
	}

	private void removeEvents(List<CIEvent> eventsToRemove) {
		if (eventsToRemove != null && !eventsToRemove.isEmpty()) {
			synchronized (events) {
				events.removeAll(eventsToRemove);
			}
		}
	}

	//  infallible everlasting worker function
	private void worker() {
		while (true) {
			//  have any events to send?
			if (events.isEmpty()) {
				doBreakableWait(NO_EVENTS_PAUSE);
				continue;
			}

			//  get and validate Octane configuration
			OctaneConfiguration octaneConfiguration;
			try {
				octaneConfiguration = pluginServices.getOctaneConfiguration();
				if (octaneConfiguration == null || !octaneConfiguration.isValid()) {
					logger.info("failed to obtain Octane configuration, pausing for " + OCTANE_CONFIGURATION_UNAVAILABLE_PAUSE + "ms...");
					doBreakableWait(OCTANE_CONFIGURATION_UNAVAILABLE_PAUSE);
					logger.info("back from pause");
					continue;
				}
			} catch (Throwable t) {
				logger.error("failed to obtain Octane configuration, pausing for " + OCTANE_CONFIGURATION_UNAVAILABLE_PAUSE + "ms...", t);
				doBreakableWait(OCTANE_CONFIGURATION_UNAVAILABLE_PAUSE);
				logger.info("back from pause");
				continue;
			}

			//  build events list to be sent
			List<CIEvent> eventsChunk = null;
			CIEventsList eventsSnapshot;
			try {
				synchronized (events) {
					eventsChunk = new ArrayList<>(events.subList(0, Math.min(events.size(), EVENTS_CHUNK_SIZE)));
				}
				eventsSnapshot = dtoFactory.newDTO(CIEventsList.class)
						.setServer(pluginServices.getServerInfo())
						.setEvents(eventsChunk);
			} catch (Throwable t) {
				logger.error("failed to serialize chunk of " + (eventsChunk != null ? eventsChunk.size() : "[NULL]") + " events, dropping them off (if any) and continue");
				if (eventsChunk != null && !eventsChunk.isEmpty()) {
					removeEvents(eventsChunk);
				}
				continue;
			}

			//  send the data to Octane
			try {
				logEventsToBeSent(octaneConfiguration, eventsSnapshot);
				sendEventsData(octaneConfiguration, eventsSnapshot);
				removeEvents(eventsChunk);
				logger.info("... done, left to send " + events.size() + " events");
			} catch (TemporaryException tqie) {
				logger.error("failed to send events with temporary error, breathing " + TEMPORARY_FAILURE_PAUSE + "ms and continue", tqie);
				doBreakableWait(TEMPORARY_FAILURE_PAUSE);
			} catch (PermanentException pqie) {
				logger.error("failed to send events with permanent error, dropping this chunk and continue", pqie);
				removeEvents(eventsChunk);
			} catch (Throwable t) {
				logger.error("failed to send events with unexpected error, dropping this chunk and continue", t);
				removeEvents(eventsChunk);
			}
		}
	}

	private void logEventsToBeSent(OctaneConfiguration configuration, CIEventsList eventsList) {
		try {
			String targetOctane = configuration.getUrl() + ", SP: " + configuration.getSharedSpace();
			String eventsSummary = String.join(", ", eventsList.getEvents().stream().map(e -> e.getProject() + ":" + e.getBuildCiId() + ":" + e.getEventType()).collect(Collectors.toSet()));
			logger.info("sending [" + eventsSummary + "] event/s to [" + targetOctane + "]...");
		} catch (Exception e) {
			logger.error("failed to log events to be sent", e);
		}
	}

	private void sendEventsData(OctaneConfiguration configuration, CIEventsList eventsList) {
		Map<String, String> headers = new HashMap<>();
		headers.put(CONTENT_TYPE_HEADER, ContentType.APPLICATION_JSON.getMimeType());
		OctaneRequest octaneRequest = dtoFactory.newDTO(OctaneRequest.class)
				.setMethod(HttpMethod.PUT)
				.setUrl(configuration.getUrl() +
						SHARED_SPACE_INTERNAL_API_PATH_PART + configuration.getSharedSpace() +
						ANALYTICS_CI_PATH_PART + "events")
				.setHeaders(headers)
				.setBody(dtoFactory.dtoToJsonStream(eventsList));
		OctaneResponse octaneResponse;
		try {
			octaneResponse = restService.obtainClient().execute(octaneRequest);
		} catch (IOException ioe) {
			throw new TemporaryException(ioe);
		}
		if (octaneResponse.getStatus() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
			throw new TemporaryException("PUT events failed with status " + octaneResponse.getStatus());
		} else if (octaneResponse.getStatus() != HttpStatus.SC_OK) {
			throw new PermanentException("PUT events failed with status " + octaneResponse.getStatus());
		}
	}

	private void doBreakableWait(long period) {
		try {
			Thread.sleep(period);
		} catch (InterruptedException ie) {
			logger.warn("interrupted while waiting", ie);
		}
	}

	private static final class EventsServiceWorkerThreadFactory implements ThreadFactory {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("EventsServiceWorker-" + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}
