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

package com.hp.octane.integrations.services.pullrequestsandbranches.factory;

import java.io.Serializable;

public class PullRequestFetchParameters implements Serializable {
    private String repoUrl;
    private String sourceBranchFilter;
    private String targetBranchFilter;
    private Integer pageSize;
    private Integer maxPRsToFetch;
    private Integer maxCommitsToFetch;
    private Long minUpdateTime;

    public static final int DEFAULT_PAGE_SIZE = 30;
    public static final int DEFAULT_MAX_PRS = 100;
    public static final int DEFAULT_MAX_COMMITS = 100;
    public static final long DEFAULT_MIN_UPDATE_DATE = 999;


    public String getRepoUrl() {
        return repoUrl;
    }

    public PullRequestFetchParameters setRepoUrl(String repoUrl) {
        this.repoUrl = repoUrl;
        return this;
    }

    public String getSourceBranchFilter() {
        return sourceBranchFilter;
    }

    public PullRequestFetchParameters setSourceBranchFilter(String sourceBranchFilter) {
        this.sourceBranchFilter = sourceBranchFilter;
        return this;
    }

    public String getTargetBranchFilter() {
        return targetBranchFilter;
    }

    public PullRequestFetchParameters setTargetBranchFilter(String targetBranchFilter) {
        this.targetBranchFilter = targetBranchFilter;
        return this;
    }

    public int getPageSize() {
        return pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
    }

    public PullRequestFetchParameters setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public int getMaxPRsToFetch() {
        return maxPRsToFetch == null ? DEFAULT_MAX_PRS : maxPRsToFetch;
    }

    public PullRequestFetchParameters setMaxPRsToFetch(Integer maxPRsToFetch) {
        this.maxPRsToFetch = maxPRsToFetch;
        return this;
    }

    public int getMaxCommitsToFetch() {
        return maxCommitsToFetch == null ? DEFAULT_MAX_COMMITS : maxCommitsToFetch;
    }

    public PullRequestFetchParameters setMaxCommitsToFetch(Integer maxCommitsToFetch) {
        this.maxCommitsToFetch = maxCommitsToFetch;
        return this;
    }

    public long getMinUpdateTime() {return minUpdateTime == null ? DEFAULT_MIN_UPDATE_DATE : minUpdateTime;
    }

    public PullRequestFetchParameters setMinUpdateTime(Long minUpdateTime) {
        this.minUpdateTime = minUpdateTime;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("\n");
        sb.append("Min update date   : ").append(getMinUpdateTime()).append("\n");
        sb.append("Source branch filter : ").append(getSourceBranchFilter()).append("\n");
        sb.append("Target branch filter : ").append(getTargetBranchFilter()).append("\n");
        sb.append("Max PRs to fetch     : ").append(getMaxPRsToFetch()).append("\n");
        sb.append("Max commits to fetch : ").append(getMaxCommitsToFetch()).append("\n");
        sb.append("Page size            : ").append(getPageSize()).append("\n");
        return sb.toString();
    }
}
