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

package com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchUtils;
import com.hp.octane.integrations.services.pullrequestsandbranches.github.GithubV3FetchHandler;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PullRequest extends Entity implements SupportUpdatedTime {
    private int number;
    private String htmlUrl;
    private String commitsUrl;
    private String state;
    private String title;
    private String body;

    private String createdAt;
    private String updatedAt;
    private String closedAt;
    private String mergedAt;

    private PullRequestUser user;

    private PullRequestRepo base;
    private PullRequestRepo head;

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    @JsonSetter("created_at")
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public long getUpdatedTime() {
        Long l = FetchUtils.convertISO8601DateStringToLong(updatedAt);
        return l == null ? 0 : l;
    }

    @JsonSetter("updated_at")
    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getClosedAt() {
        return closedAt;
    }

    @JsonSetter("closed_at")
    public void setClosedAt(String closedAt) {
        this.closedAt = closedAt;
    }

    public String getMergedAt() {
        return mergedAt;
    }

    @JsonSetter("merged_at")
    public void setMergedAt(String mergedAt) {
        this.mergedAt = mergedAt;
    }

    public PullRequestUser getUser() {
        return user;
    }

    public void setUser(PullRequestUser user) {
        this.user = user;
    }

    public PullRequestRepo getBase() {
        return base;
    }

    public void setBase(PullRequestRepo base) {
        this.base = base;
    }

    public PullRequestRepo getHead() {
        return head;
    }

    public void setHead(PullRequestRepo head) {
        this.head = head;
    }

    public String getCommitsUrl() {
        return commitsUrl;
    }

    @JsonSetter("commits_url")
    public void setCommitsUrl(String commitsUrl) {
        this.commitsUrl = commitsUrl;
    }

    public String getHtmlUrl() {
        return htmlUrl;
    }

    @JsonSetter("html_url")
    public void setHtmlUrl(String htmlUrl) {
        this.htmlUrl = htmlUrl;
    }
}
