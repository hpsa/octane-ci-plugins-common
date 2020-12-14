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
import com.hp.octane.integrations.services.pullrequestsandbranches.github.GithubV3FetchHandler;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Commit extends Entity implements SupportUpdatedTime {

    private String sha;
    private CommitDetails commit;
    private List<CommitParent> parents;

    private long UPDATE_TIME_DEFAULT = -1;
    private long updatedTime = UPDATE_TIME_DEFAULT;

    public String getSha() {
        return sha;
    }

    public void setSha(String sha) {
        this.sha = sha;
    }

    public CommitDetails getCommit() {
        return commit;
    }

    public void setCommit(CommitDetails commit) {
        this.commit = commit;
    }

    public List<CommitParent> getParents() {
        return parents;
    }

    public void setParents(List<CommitParent> parents) {
        this.parents = parents;
    }

    @Override
    public long getUpdatedTime() {
        if (updatedTime == UPDATE_TIME_DEFAULT) {
            Long l = GithubV3FetchHandler.convertDateToLong(getCommit().getCommitter().getDate());
            updatedTime = l == null ? 0 : l;
        }
        return updatedTime;
    }
}
