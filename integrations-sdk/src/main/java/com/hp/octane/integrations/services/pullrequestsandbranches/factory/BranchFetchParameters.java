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

public class BranchFetchParameters implements Serializable {
    private String repoUrl;
    private String filter;
    private Integer pageSize;
    private Integer activeBranchDays;
    private Integer maxBranchesToFill;

    public static final int DEFAULT_PAGE_SIZE = 1000;
    public static final int DEFAULT_MAX_BRANCHES_TO_FILL = 1000;
    public static final int DEFAULT_ACTIVE_BRANCH_DAYS = 90;

    public String getRepoUrl() {
        return repoUrl;
    }

    public BranchFetchParameters setRepoUrl(String repoUrl) {
        this.repoUrl = repoUrl;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public BranchFetchParameters setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public int getMaxBranchesToFill() {

        return maxBranchesToFill == null ? DEFAULT_MAX_BRANCHES_TO_FILL : maxBranchesToFill;
    }

    public BranchFetchParameters setMaxBranchesToFill(Integer maxBranchesToFill) {
        this.maxBranchesToFill = maxBranchesToFill;
        return this;
    }

    public int getActiveBranchDays() {
        return activeBranchDays == null ? DEFAULT_ACTIVE_BRANCH_DAYS : activeBranchDays;
    }

    public BranchFetchParameters setActiveBranchDays(Integer activeBranchDays) {
        this.activeBranchDays = activeBranchDays;
        return this;
    }

    public int getPageSize() {
        return pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
    }

    public BranchFetchParameters setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("\n");
        sb.append("Filter       : ").append(getFilter()).append("\n");
        sb.append("Page size    : ").append(getPageSize()).append("\n");
        return sb.toString();
    }
}
