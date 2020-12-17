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
import com.hp.octane.integrations.dto.entities.Entity;
import com.hp.octane.integrations.dto.entities.EntityConstants;
import com.hp.octane.integrations.dto.scm.Branch;
import com.hp.octane.integrations.dto.scm.PullRequest;
import com.hp.octane.integrations.services.entities.EntitiesService;
import com.hp.octane.integrations.services.entities.QueryHelper;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.BranchFetchParameters;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.CommitUserIdPicker;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchUtils;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Default implementation of tests service
 */

final class PullRequestAndBranchServiceImpl implements PullRequestAndBranchService {
    private static final Logger logger = LogManager.getLogger(PullRequestAndBranchServiceImpl.class);
    private static final DTOFactory dtoFactory = DTOFactory.getInstance();

    private final OctaneSDK.SDKServicesConfigurer configurer;
    private final RestService restService;
    private final EntitiesService entitiesService;
    private final File persistenceFile;
    private Map<String, PRItem> prItems;
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);


    PullRequestAndBranchServiceImpl(OctaneSDK.SDKServicesConfigurer configurer, RestService restService, EntitiesService entitiesService) {
        if (configurer == null) {
            throw new IllegalArgumentException("invalid configurer");
        }
        if (restService == null) {
            throw new IllegalArgumentException("rest service MUST NOT be null");
        }
        if (entitiesService == null) {
            throw new IllegalArgumentException("entities service MUST NOT be null");
        }
        this.configurer = configurer;
        this.restService = restService;
        this.entitiesService = entitiesService;

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
        savePullRequestLastUpdateTime(workspaceId, pullRequestFetchParameters.getRepoUrl(), lastUpdateTime);
        logConsumer.accept("Last update time set to " + lastUpdateTime);
    }

    @Override
    public long getPullRequestLastUpdateTime(String workspaceId, String repoUrl) {
        String key = PRItem.buildKey(workspaceId, repoUrl);
        PRItem item = prItems.get(key);
        return item == null ? 0 : item.getLastUpdated();
    }

    @Override
    public void syncBranchesToOctane(List<Branch> ciServerBranches, BranchFetchParameters fp, Long workspaceId, CommitUserIdPicker idPicker, Consumer<String> logConsumer) {
        Map<String, Branch> ciServerBranchMap = ciServerBranches.stream().collect(Collectors.toMap(Branch::getName, Function.identity()));


        //GET BRANCHES FROM OCTANE
        List<Entity> roots = getRepositoryRoots(fp, workspaceId);
        List<Entity> octaneBranches = null;
        String rootId = null;
        if (roots != null && !roots.isEmpty()) {
            rootId = roots.get(0).getId();
            octaneBranches = getRepositoryBranches(rootId, workspaceId);
        } else {
            //TODO create root
        }
        if (octaneBranches == null) {
            octaneBranches = Collections.emptyList();
        }
        Map<String, List<Entity>> octaneBranchMap = octaneBranches.stream().collect(groupingBy(b -> getOctaneBranchName(b)));


        //GENERATE UPDATES
        List<Entity> toUpdate = new ArrayList<>();
        List<Entity> toCreate = new ArrayList<>();

        List<Entity> deleted = octaneBranchMap.entrySet().stream().filter(entry -> !ciServerBranchMap.containsKey(entry.getKey()))
                .map(e -> e.getValue()).flatMap(Collection::stream).map(e -> buildOctaneBranchForUpdateAsDeleted(e))
                .collect(Collectors.toList());
        toUpdate.addAll(deleted);

        String finalRootId = rootId;
        ciServerBranches.forEach(ciBranch -> {
            List<Entity> octaneBranchList = octaneBranchMap.get(ciBranch.getName());
            if (octaneBranchList == null) {//not exist in octane, check if to add
                long diff = System.currentTimeMillis() - ciBranch.getLastCommitTime();
                long diffDays = TimeUnit.MILLISECONDS.toDays(diff);
                if (diffDays < fp.getActiveBranchDays()) {
                    toCreate.add(buildOctaneBranchForCreate(finalRootId, ciBranch, idPicker));
                }
            } else {//check for update
                octaneBranchList.forEach(octaneBranch -> {
                    if (!ciBranch.getLastCommitSHA().equals(octaneBranch.getField(EntityConstants.ScmRepository.LAST_COMMIT_SHA_FIELD)) ||
                            !ciBranch.getIsMerged().equals(octaneBranch.getField(EntityConstants.ScmRepository.IS_MERGED_FIELD))) {
                        toUpdate.add(buildOctaneBranchForUpdate(octaneBranch, ciBranch, idPicker));
                    }
                });
            }
        });

        //SEND TO OCTANE
        entitiesService.updateEntities(workspaceId, EntityConstants.ScmRepository.COLLECTION_NAME, toUpdate);
        entitiesService.postEntities(workspaceId, EntityConstants.ScmRepository.COLLECTION_NAME, toCreate);
    }

    private String getOctaneBranchName(Entity octaneBranch) {
        //branch name might be with prefix for example, "refs/remotes/origin/master", add new field that contains branch name with prefixes
        String branchName = (String) octaneBranch.getField(EntityConstants.ScmRepository.BRANCH_FIELD);
        int slashIndex = branchName.lastIndexOf("/");
        String shortBranchName = (slashIndex > 0) ? branchName.substring(slashIndex) : branchName;
        return shortBranchName;
    }

    private Entity buildOctaneBranchForUpdateAsDeleted(Entity octaneBranch) {
        Entity entity = DTOFactory.getInstance().newDTO(Entity.class);
        entity.setType(EntityConstants.ScmRepository.ENTITY_NAME);
        entity.setId(octaneBranch.getId());
        entity.setField(EntityConstants.ScmRepository.IS_DELETED_FIELD, true);
        return entity;
    }

    private Entity buildOctaneBranchForUpdate(Entity octaneBranch, Branch ciBranch, CommitUserIdPicker idPicker) {
        Entity entity = DTOFactory.getInstance().newDTO(Entity.class);
        entity.setType(EntityConstants.ScmRepository.ENTITY_NAME);
        if (octaneBranch != null) {
            entity.setId(octaneBranch.getId());
        }

        entity.setField(EntityConstants.ScmRepository.IS_MERGED_FIELD, ciBranch.getIsMerged());
        entity.setField(EntityConstants.ScmRepository.LAST_COMMIT_SHA_FIELD, ciBranch.getLastCommitSHA());
        entity.setField(EntityConstants.ScmRepository.LAST_COMMIT_TIME_FIELD, FetchUtils.convertLongToISO8601DateString(ciBranch.getLastCommitTime()));
        entity.setField(EntityConstants.ScmRepository.SCM_USER_FIELD, idPicker.getUserIdForCommit(ciBranch.getLastCommiterEmail(), ciBranch.getLastCommiterName()));
        return entity;
    }

    private Entity buildOctaneBranchForCreate(String rootId, Branch ciBranch, CommitUserIdPicker idPicker) {
        Entity parent = DTOFactory.getInstance().newDTO(Entity.class);
        parent.setType(EntityConstants.ScmRepositoryRoot.ENTITY_NAME);
        parent.setId(rootId);

        Entity entity = buildOctaneBranchForUpdate(null, ciBranch, idPicker);
        entity.setField(EntityConstants.ScmRepository.BRANCH_FIELD, ciBranch.getName());
        entity.setField(EntityConstants.ScmRepository.PARENT_FIELD, parent);
        return entity;
    }

    private List<Entity> getRepositoryRoots(BranchFetchParameters parameters, Long workspaceId) {
        String rootByUrlCondition = QueryHelper.condition(EntityConstants.ScmRepositoryRoot.URL_FIELD, parameters.getRepoUrl());
        List<Entity> foundRoots = entitiesService.getEntities(workspaceId,
                EntityConstants.ScmRepositoryRoot.COLLECTION_NAME,
                Collections.singleton(rootByUrlCondition),
                Collections.singletonList(EntityConstants.Base.ID_FIELD));
        return foundRoots;
    }

    private List<Entity> getRepositoryBranches(String repositoryRootId, Long workspaceId) {
        String byParentIdCondition = QueryHelper.conditionRef(EntityConstants.ScmRepository.PARENT_FIELD, Long.parseLong(repositoryRootId));
        String notDeletedCondition = QueryHelper.condition(EntityConstants.ScmRepository.IS_DELETED_FIELD, false);
        List<Entity> foundBranches = entitiesService.getEntities(workspaceId,
                EntityConstants.ScmRepository.COLLECTION_NAME,
                Arrays.asList(byParentIdCondition, notDeletedCondition),
                Arrays.asList(EntityConstants.ScmRepository.BRANCH_FIELD,
                        EntityConstants.ScmRepository.IS_MERGED_FIELD,
                        EntityConstants.ScmRepository.LAST_COMMIT_SHA_FIELD,
                        EntityConstants.ScmRepository.LAST_COMMIT_TIME_FIELD));

        return foundBranches;
    }

    private synchronized void savePullRequestLastUpdateTime(String workspaceId, String repoUrl, long lastUpdateTime) {
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
