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

package com.hp.octane.integrations.dto.scm.impl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hp.octane.integrations.dto.scm.Branch;

/**
 * SCMCommit DTO
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BranchImpl implements Branch {

    private String name;
    private Boolean isMerged;
    private String lastCommitSHA;
    private Long lastCommitTime;
    private String lastCommiterName;
    private String lastCommiterEmail;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Branch setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public Boolean getIsMerged() {
        return isMerged;
    }

    @Override
    public Branch setIsMerged(boolean isMerged) {
        this.isMerged = isMerged;
        return this;
    }

    @Override
    public String getLastCommitSHA() {
        return lastCommitSHA;
    }

    @Override
    public Branch setLastCommitSHA(String lastCommitSHA) {
        this.lastCommitSHA = lastCommitSHA;
        return this;
    }

    @Override
    public Long getLastCommitTime() {
        return lastCommitTime;
    }

    @Override
    public Branch setLastCommitTime(Long lastCommitTime) {
        this.lastCommitTime = lastCommitTime;
        return this;
    }

    @Override
    public String getLastCommiterName() {
        return lastCommiterName;
    }

    @Override
    public Branch setLastCommiterName(String lastCommiterName) {
        this.lastCommiterName = lastCommiterName;
        return this;
    }

    @Override
    public String getLastCommiterEmail() {
        return lastCommiterEmail;
    }

    @Override
    public Branch setLastCommiterEmail(String lastCommiterEmail) {
        this.lastCommiterEmail = lastCommiterEmail;
        return this;
    }
}
