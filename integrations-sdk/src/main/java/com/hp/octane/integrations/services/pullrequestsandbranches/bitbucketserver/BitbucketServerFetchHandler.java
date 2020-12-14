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
package com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver;

import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.scm.SCMRepository;
import com.hp.octane.integrations.dto.scm.SCMType;
import com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.*;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.CommitUserIdPicker;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.PullRequestFetchParameters;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchUtils;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchHandler;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;
import org.apache.http.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BitbucketServerFetchHandler extends FetchHandler {

    public BitbucketServerFetchHandler(AuthenticationStrategy authenticationStrategy) {
        super(authenticationStrategy);
    }

    @Override
    public List<com.hp.octane.integrations.dto.scm.PullRequest> fetchPullRequests(PullRequestFetchParameters parameters, CommitUserIdPicker commitUserIdPicker, Consumer<String> logConsumer) throws IOException {

        List<com.hp.octane.integrations.dto.scm.PullRequest> result = new ArrayList<>();
        String baseUrl = getRepoApiPath(parameters.getRepoUrl());
        logConsumer.accept("BitbucketServerRestHandler, Base url : " + baseUrl);
        pingRepository(baseUrl, logConsumer);

        String pullRequestsUrl = baseUrl + "/pull-requests?state=ALL";
        logConsumer.accept("Pull requests url : " + pullRequestsUrl);

        List<PullRequest> pullRequests = getPagedEntities(pullRequestsUrl, PullRequest.class, parameters.getPageSize(), parameters.getMaxPRsToFetch(), parameters.getMinUpdateTime());
        List<Pattern> sourcePatterns = FetchUtils.buildPatterns(parameters.getSourceBranchFilter());
        List<Pattern> targetPatterns = FetchUtils.buildPatterns(parameters.getTargetBranchFilter());

        List<PullRequest> filteredPullRequests = pullRequests.stream()
                .filter(pr -> FetchUtils.isBranchMatch(sourcePatterns, pr.getFromRef().getDisplayId()) && FetchUtils.isBranchMatch(targetPatterns, pr.getToRef().getDisplayId()))
                .collect(Collectors.toList());
        logConsumer.accept(String.format("Received %d pull-requests, while %d are matching source/target filters", pullRequests.size(), filteredPullRequests.size()));

        if (!filteredPullRequests.isEmpty()) {
            logConsumer.accept("Fetching commits ...");
            int counter = 0;
            for (PullRequest pr : filteredPullRequests) {
                String url = baseUrl + "/pull-requests/" + pr.getId() + "/commits";
                List<Commit> commits = getPagedEntities(url, Commit.class, parameters.getPageSize(), parameters.getMaxCommitsToFetch(), parameters.getMinUpdateTime());

                List<com.hp.octane.integrations.dto.scm.SCMCommit> dtoCommits = new ArrayList<>();
                for (Commit commit : commits) {
                    com.hp.octane.integrations.dto.scm.SCMCommit dtoCommit = dtoFactory.newDTO(com.hp.octane.integrations.dto.scm.SCMCommit.class)
                            .setRevId(commit.getId())
                            .setComment(commit.getMessage())
                            .setUser(getUserName(commit.getCommitter().getEmailAddress(), commit.getCommitter().getName()))
                            .setUserEmail(commit.getCommitter().getEmailAddress())
                            .setTime(commit.getCommitterTimestamp())
                            .setParentRevId(commit.getParents().get(0).getId());
                    dtoCommits.add(dtoCommit);
                }

                SCMRepository sourceRepository = buildScmRepository(pr.getFromRef());
                SCMRepository targetRepository = buildScmRepository(pr.getToRef());

                boolean isMerged = PullRequest.MERGED_STATE.equals(pr.getState());
                String userId = getUserName(commitUserIdPicker, pr.getAuthor().getUser().getEmailAddress(), pr.getAuthor().getUser().getName());
                com.hp.octane.integrations.dto.scm.PullRequest dtoPullRequest = dtoFactory.newDTO(com.hp.octane.integrations.dto.scm.PullRequest.class)
                        .setId(pr.getId())
                        .setTitle(pr.getTitle())
                        .setDescription(pr.getDescription())
                        .setState(pr.getState())
                        .setCreatedTime(pr.getCreatedDate())
                        .setUpdatedTime(pr.getUpdatedTime())
                        .setAuthorName(userId)
                        .setAuthorEmail(pr.getAuthor().getUser().getEmailAddress())
                        .setClosedTime(pr.getClosedDate())
                        .setSelfUrl(pr.getLinks().getSelf().get(0).getHref())
                        .setSourceRepository(sourceRepository)
                        .setTargetRepository(targetRepository)
                        .setCommits(dtoCommits)
                        .setMergedTime(isMerged ? pr.getClosedDate() : null)
                        .setIsMerged(isMerged);
                result.add(dtoPullRequest);

                if (counter > 0 && counter % 40 == 0) {
                    logConsumer.accept("Fetching commits " + counter * 100 / filteredPullRequests.size() + "%");
                }
                counter++;
            }
            logConsumer.accept("Fetching commits is done");
            logConsumer.accept("Pull requests are ready");
        } else {
            logConsumer.accept("No new/updated PR is found.");
        }
        return result;
    }

    private SCMRepository buildScmRepository(Ref ref) {
        Optional<Link> optLink = ref.getRepository().getLinks().getClone().stream().filter(l -> !l.getName().toLowerCase().equals("ssh")).findFirst();
        String url = optLink.isPresent() ? optLink.get().getHref() : ref.getRepository().getLinks().getClone().get(0).getHref();
        return dtoFactory.newDTO(SCMRepository.class)
                .setUrl(url)
                .setBranch(ref.getDisplayId())
                .setType(SCMType.GIT);
    }

    private <T extends Entity & SupportUpdatedTime> List<T> getPagedEntities(String url, Class<T> entityType, int pageSize, int maxTotal, Long minUpdateTime) {
        try {
            //https://developer.atlassian.com/server/confluence/pagination-in-the-rest-api/
            List<T> result = new ArrayList<>();
            boolean finished;
            int limit = pageSize;
            int start = 0;
            do {
                String myUrl = url + (url.contains("?") ? "" : "?") + String.format("&limit=%d&start=%d", limit, start);
                OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class).setUrl(myUrl).setMethod(HttpMethod.GET);
                OctaneResponse response = restClient.executeRequest(request);
                if (response.getStatus() != HttpStatus.SC_OK) {
                    throw new RuntimeException(String.format("Request to '%s' is ended with result %d : %s", myUrl, response.getStatus(), JsonConverter.getErrorMessage(response.getBody())));
                }
                EntityCollection<T> collection = JsonConverter.convertCollection(response.getBody(), entityType);
                result.addAll(collection.getValues());
                finished = collection.isLastPage() || result.size() > maxTotal;
                limit = collection.getLimit();
                start = collection.getStart() + collection.getLimit();

                //remove outdated items
                for (int i = result.size() - 1; i >= 0; i--) {
                    if (result.get(i).getUpdatedTime() <= minUpdateTime) {
                        result.remove(i);
                        finished = true;
                    } else {
                        break;
                    }
                }
            } while (!finished);

            //remove exceeded items
            while (result.size() > maxTotal) {
                result.remove(result.size() - 1);
            }
            return result;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to getPagedEntities : " + e.getMessage(), e);
        }
    }

    @Override
    public String getRepoApiPath(String repoHttpCloneUrl) {
        validateHttpCloneUrl(repoHttpCloneUrl);

        //http://localhost:8990/scm/proj/rep1.git   =>http://localhost:8990/rest/api/1.0/projects/proj/repos/rep1
        //http://localhost:8990/scm/~admin/rep1.git =>http://localhost:8990/rest/api/1.0/users/admin/repos/rep1
        try {
            List<String> parts = Arrays.asList(repoHttpCloneUrl.trim().split("/"));

            int scmIndex = repoHttpCloneUrl.toLowerCase().indexOf("/scm/");
            StringBuilder sb = new StringBuilder();

            sb.append(repoHttpCloneUrl, 0, scmIndex);
            sb.append("/rest/api/1.0");

            //add project or username
            String projOrUserPart = parts.get(parts.size() - 2);
            if (projOrUserPart.startsWith("~")) {
                //set /users/userName
                sb.append("/users/");
                sb.append(projOrUserPart.substring(1));//remove ~
            } else {
                //set /projects/projName
                sb.append("/projects/");
                sb.append(projOrUserPart);
            }

            //add repo name without .git
            String repoPart = parts.get(parts.size() - 1);
            if (repoPart.toLowerCase().endsWith(".git")) {
                repoPart = repoPart.substring(0, repoPart.length() - 4);//remove ".git"
            }
            sb.append("/repos/");
            sb.append(repoPart);

            return sb.toString();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unexpected bitbucket server repository URL : " + repoHttpCloneUrl + ". Expected formats : http(s)://<bitbucket_server>:<port>/scm/<project_name>/<repository_name>.git" +
                    " or  http(s)://<bitbucket_server>:<port>/scm/~<user_name>/<repository_name>.git");
        }
    }

    @Override
    protected String parseRequestError(OctaneResponse response) {
        return JsonConverter.getErrorMessage(response.getBody());
    }
}
