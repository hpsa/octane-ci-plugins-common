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

package com.hp.octane.integrations.services.pullrequestsandbranches;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.scm.PullRequest;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.PullRequestFetchParameters;
import com.hp.octane.integrations.services.rest.RestService;
import org.apache.commons.collections4.ListUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation of tests service
 */

final class PullRequestAndBranchServiceImpl implements PullRequestAndBranchService {
    private static final Logger logger = LogManager.getLogger(PullRequestAndBranchServiceImpl.class);
    private static final DTOFactory dtoFactory = DTOFactory.getInstance();

    private final OctaneSDK.SDKServicesConfigurer configurer;
    private final RestService restService;
    private final File persistenceFile;
    private Map<String, PRItem> prItems;
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);


    PullRequestAndBranchServiceImpl(OctaneSDK.SDKServicesConfigurer configurer, RestService restService) {
        if (configurer == null) {
            throw new IllegalArgumentException("invalid configurer");
        }
        if (restService == null) {
            throw new IllegalArgumentException("rest service MUST NOT be null");
        }
        this.configurer = configurer;
        this.restService = restService;
        logger.info(configurer.octaneConfiguration.geLocationForLog() + "initialized SUCCESSFULLY");

        if (configurer.pluginServices.getAllowedOctaneStorage() != null) {
            File storageDirectory = new File(configurer.pluginServices.getAllowedOctaneStorage(), "nga" + File.separator + configurer.octaneConfiguration.getInstanceId());
            if (!storageDirectory.mkdirs()) {
                logger.debug(configurer.octaneConfiguration.geLocationForLog() + "instance folder considered as exist");
            }
            persistenceFile = new File(storageDirectory, "pr-fetchers.json");
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "hosting plugin PROVIDE available storage, PR persistence enabled");

            if (persistenceFile.exists()) {
                try {
                    JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, PRItem.class);
                    List<PRItem> list = objectMapper.readValue(persistenceFile, type);
                    prItems = list.stream().collect(Collectors.toMap(PRItem::getKey, Function.identity()));
                } catch (IOException e) {
                    logger.info(configurer.octaneConfiguration.geLocationForLog() + "failed to read PR persisted file");
                }
            } else {
                prItems = new HashMap<>();
            }
        } else {
            persistenceFile = null;
            prItems = new HashMap<>();
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "hosting plugin DO NOT PROVIDE available storage, PR persistence disabled");
        }
    }

    @Override
    public void sendPullRequests(List<PullRequest> pullRequests, String workspaceId, PullRequestFetchParameters pullRequestFetchParameters, Consumer<String> logConsumer) throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(RestService.CONTENT_TYPE_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        logConsumer.accept("Sending to ALM Octane : " + configurer.octaneConfiguration.geLocationForLog() + ", workspace " + workspaceId);

        String url = configurer.octaneConfiguration.getUrl() +
                RestService.SHARED_SPACE_API_PATH_PART + configurer.octaneConfiguration.getSharedSpace() +
                "/workspaces/" + workspaceId + RestService.ANALYTICS_CI_PATH_PART + "pull-requests/";

        int sentCounter = 0;
        List<List<PullRequest>> subSets = ListUtils.partition(pullRequests, 200);
        for (List<PullRequest> list : subSets) {
            String json = dtoFactory.dtoCollectionToJson(list);
            OctaneRequest octaneRequest = dtoFactory.newDTO(OctaneRequest.class)
                    .setMethod(HttpMethod.PUT)
                    .setUrl(url)
                    .setHeaders(headers)
                    .setBody(json);

            OctaneResponse octaneResponse = restService.obtainOctaneRestClient().execute(octaneRequest);
            if (octaneResponse.getStatus() != HttpStatus.SC_OK) {
                if (octaneResponse.getStatus() == HttpStatus.SC_NOT_FOUND) {
                    throw new RuntimeException("Failed to sendPullRequests : received 404 status. Validate that you have ALM Octane version that is greater than 15.0.48");
                } else {
                    throw new RuntimeException("Failed to sendPullRequests : (" + octaneResponse.getStatus() + ")" + octaneResponse.getBody());
                }
            } else {
                sentCounter += list.size();
                logConsumer.accept(String.format("Sent %s/%s pull requests.", sentCounter, pullRequests.size()));
            }
        }

        long lastUpdateTime = pullRequests.stream().map(PullRequest::getUpdatedTime).max(Comparator.naturalOrder()).orElse(0L);
        saveLastUpdateTime(workspaceId, pullRequestFetchParameters.getRepoUrl(), lastUpdateTime);
        logConsumer.accept("Last update time set to " + lastUpdateTime);
    }

    @Override
    public long getLastUpdateTime(String workspaceId, String repoUrl) {
        String key = PRItem.buildKey(workspaceId, repoUrl);
        PRItem item = prItems.get(key);
        return item == null ? 0 : item.getLastUpdated();
    }

    public synchronized void saveLastUpdateTime(String workspaceId, String repoUrl, long lastUpdateTime) {
        PRItem item = PRItem.create(workspaceId, repoUrl, lastUpdateTime);
        prItems.put(item.getKey(), item);
        if (persistenceFile != null) {
            try {
                objectMapper.writeValue(persistenceFile, prItems.values());
            } catch (IOException e) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "failed to save PR persisted file");
            }
        }
    }

    public static class PRItem implements Serializable {
        private String workspace;
        private String repositoryUrl;
        private long lastUpdated;

        public static PRItem create(String workspace, String repositoryUrl, long lastUpdated) {
            PRItem item = new PRItem();
            item.workspace = workspace;
            item.repositoryUrl = repositoryUrl;
            item.lastUpdated = lastUpdated;
            return item;
        }

        @JsonIgnore
        public String getKey() {
            return buildKey(getWorkspace(), getRepositoryUrl());
        }

        public static String buildKey(String workspace, String repositoryUrl) {
            return workspace + "_" + repositoryUrl;
        }

        public String getWorkspace() {
            return workspace;
        }

        public String getRepositoryUrl() {
            return repositoryUrl;
        }

        public long getLastUpdated() {
            return lastUpdated;
        }
    }
}
