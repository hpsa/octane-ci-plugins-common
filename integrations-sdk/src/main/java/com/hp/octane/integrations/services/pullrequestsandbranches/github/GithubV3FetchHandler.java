
package com.hp.octane.integrations.services.pullrequestsandbranches.github;

import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.scm.SCMRepository;
import com.hp.octane.integrations.dto.scm.SCMType;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.CommitUserIdPicker;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.PullRequestFetchParameters;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchUtils;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchHandler;
import com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.*;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class GithubV3FetchHandler extends FetchHandler {

    public static final String CLOUD_SERVICE_PREFIX = "https://github.com/";

    public GithubV3FetchHandler(AuthenticationStrategy authenticationStrategy) {
        super(authenticationStrategy);
    }

    @Override
    protected String parseRequestError(OctaneResponse response) {
        return JsonConverter.getErrorMessage(response.getBody());
    }

    @Override
    public List<com.hp.octane.integrations.dto.scm.PullRequest> fetchPullRequests(PullRequestFetchParameters parameters, CommitUserIdPicker commitUserIdPicker, Consumer<String> logConsumer) throws IOException {

        List<com.hp.octane.integrations.dto.scm.PullRequest> result = new ArrayList<>();
        String baseUrl = getRepoApiPath(parameters.getRepoUrl());
        logConsumer.accept(this.getClass().getSimpleName() + " handler, Base url : " + baseUrl);
        pingRepository(baseUrl, logConsumer);

        String pullRequestsUrl = baseUrl + "/pulls?state=all";
        logConsumer.accept("Pull requests url : " + pullRequestsUrl);

        //prs are returned in asc order by id , therefore we need to get all before filtering , therefore page size equals to max total
        List<PullRequest> pullRequests = getPagedEntities(pullRequestsUrl, PullRequest.class, parameters.getMaxPRsToFetch(), parameters.getMaxPRsToFetch(), parameters.getMinUpdateTime());
        List<Pattern> sourcePatterns = FetchUtils.buildPatterns(parameters.getSourceBranchFilter());
        List<Pattern> targetPatterns = FetchUtils.buildPatterns(parameters.getTargetBranchFilter());

        List<PullRequest> filteredPullRequests = pullRequests.stream()
                .filter(pr -> FetchUtils.isBranchMatch(sourcePatterns, pr.getHead().getRef()) && FetchUtils.isBranchMatch(targetPatterns, pr.getBase().getRef()))
                .collect(Collectors.toList());
        logConsumer.accept(String.format("Received %d pull-requests, while %d are matching source/target filters", pullRequests.size(), filteredPullRequests.size()));

        if (!filteredPullRequests.isEmpty()) {
            //users
            Set<String> userUrls = filteredPullRequests.stream().map(PullRequest::getUser).map(PullRequestUser::getUrl).collect(Collectors.toSet());
            logConsumer.accept("Fetching PR owners information ...");
            int counter = 0;
            Map<String, User> login2User = new HashMap<>();
            for (String url : userUrls) {
                User user = getEntity(url, User.class);
                login2User.put(user.getLogin(), user);
                if (counter > 0 && counter % 10 == 0) {
                    logConsumer.accept("Fetching PR owners information " + counter * 100 / userUrls.size() + "%");
                }
                counter++;
            }
            Set<String> usersWithoutMails = login2User.values().stream().filter(u -> u.getEmail() == null).map(u -> u.getLogin()).collect(Collectors.toSet());
            if (!usersWithoutMails.isEmpty()) {
                logConsumer.accept("Note : Some users doesn't have defined public email in their profile. For such users, SCM user will contain their login name:  " + usersWithoutMails);
            }
            logConsumer.accept("Fetching PR owners information is done");

            logConsumer.accept("Fetching commits ...");
            counter = 0;
            for (PullRequest pr : filteredPullRequests) {
                //commits are returned in asc order by update time , therefore we need to get all before filtering , therefore page size equals to max total
                List<Commit> commits = getPagedEntities(pr.getCommitsUrl(), Commit.class, parameters.getMaxCommitsToFetch(), parameters.getMaxCommitsToFetch(), parameters.getMinUpdateTime());

                //commits
                List<com.hp.octane.integrations.dto.scm.SCMCommit> dtoCommits = new ArrayList<>();
                for (Commit commit : commits) {
                    com.hp.octane.integrations.dto.scm.SCMCommit dtoCommit = dtoFactory.newDTO(com.hp.octane.integrations.dto.scm.SCMCommit.class)
                            .setRevId(commit.getSha())
                            .setComment(commit.getCommit().getMessage())
                            .setUser(getUserName(commit.getCommit().getCommitter().getEmail(), commit.getCommit().getCommitter().getName()))
                            .setUserEmail(commit.getCommit().getCommitter().getEmail())
                            .setTime(convertDateToLong(commit.getCommit().getCommitter().getDate()))
                            .setParentRevId(commit.getParents().get(0).getSha());
                    dtoCommits.add(dtoCommit);
                }

                SCMRepository sourceRepository = buildScmRepository(pr.getHead());
                SCMRepository targetRepository = buildScmRepository(pr.getBase());

                User prAuthor = login2User.get(pr.getUser().getLogin());
                String userId = getUserName(commitUserIdPicker, prAuthor.getEmail(), prAuthor.getLogin());
                com.hp.octane.integrations.dto.scm.PullRequest dtoPullRequest = dtoFactory.newDTO(com.hp.octane.integrations.dto.scm.PullRequest.class)
                        .setId(Integer.toString(pr.getNumber()))
                        .setTitle(pr.getTitle())
                        .setDescription(pr.getBody())
                        .setState(pr.getState())
                        .setCreatedTime(convertDateToLong(pr.getCreatedAt()))
                        .setUpdatedTime(convertDateToLong(pr.getUpdatedAt()))
                        .setMergedTime(convertDateToLong(pr.getMergedAt()))
                        .setIsMerged(pr.getMergedAt() != null)
                        .setAuthorName(userId)
                        .setAuthorEmail(prAuthor.getEmail())
                        .setClosedTime(convertDateToLong(pr.getClosedAt()))
                        .setSelfUrl(pr.getHtmlUrl())
                        .setSourceRepository(sourceRepository)
                        .setTargetRepository(targetRepository)
                        .setCommits(dtoCommits);
                result.add(dtoPullRequest);

                if (counter > 0 && counter % 25 == 0) {
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


    public static Long convertDateToLong(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return null;
        }
        //All timestamps return in ISO 8601 format:YYYY-MM-DDTHH:MM:SSZ
        return Instant.parse(dateStr).getEpochSecond() * 1000;
    }

    public static String convertLongDateToISO8601(long date) {
        //All timestamps return in ISO 8601 format:YYYY-MM-DDTHH:MM:SSZ
        return Instant.ofEpochMilli(date).toString();
    }

    private SCMRepository buildScmRepository(PullRequestRepo ref) {
        return dtoFactory.newDTO(SCMRepository.class)
                .setUrl(ref.getRepo() != null ? ref.getRepo().getClone_url() : "unknown repository")
                .setBranch(ref.getRef())
                .setType(SCMType.GIT);
    }

    /***
     *
     * @param url
     * @param entityType
     * @param pageSize
     * @param maxTotal
     * @param minUpdateTime
     * @param <T>
     * @return
     */
    private <T extends Entity & SupportUpdatedTime> List<T> getPagedEntities(String url, Class<T> entityType, int pageSize, int maxTotal, long minUpdateTime) {
        try {
            List<T> result = new ArrayList<>();
            boolean finished;
            String myUrl = url + (url.contains("?") ? "" : "?") + "&per_page=" + pageSize;

            do {
                finished = true;
                OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class).setUrl(myUrl).setMethod(HttpMethod.GET);
                OctaneResponse response = restClient.executeRequest(request);
                List<T> collection = JsonConverter.convertCollection(response.getBody(), entityType);
                result.addAll(collection);

                myUrl = getNextPageLink(response);
                if (myUrl != null) {
                    finished = false;
                }

                //remove outdated items
                for (int i = result.size() - 1; i >= 0 && minUpdateTime > 0; i--) {
                    if (result.get(i).getUpdatedTime() <= minUpdateTime) {
                        result.remove(i);
                    }
                }

                //remove exceeding items
                while (result.size() > maxTotal) {
                    result.remove(result.size() - 1);
                    finished = true;
                }
            } while (!finished);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to getPagedEntities : " + e.getMessage(), e);
        }
    }

    private <T extends Entity> T getEntity(String url, Class<T> entityType) {
        try {
            OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class).setUrl(url).setMethod(HttpMethod.GET);
            OctaneResponse response = restClient.executeRequest(request);
            return JsonConverter.convert(response.getBody(), entityType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to getEntity : " + e.getMessage(), e);
        }
    }

    private String getNextPageLink(OctaneResponse response) {
        String linkHeaderValue = response.getHeaders().get("Link");
        if (linkHeaderValue != null) {
            //<https://api.github.com/repositories/6774631/pulls?state=all&page=2>; rel="next", <https://api.github.com/repositories/6774631/pulls?state=all&page=10>; rel="last"
            String[] linksArr = linkHeaderValue.split(",");
            for (String link : linksArr) {
                if (link.endsWith("rel=\"next\"")) {
                    String[] segments = link.split(";");
                    if (segments.length == 2) {
                        String next = segments[0].trim();
                        if (next.startsWith("<") && next.endsWith(">")) {
                            return next.substring(1, next.length() - 1);
                        }
                    }
                }
            }
        }
        return null;
    }
}
