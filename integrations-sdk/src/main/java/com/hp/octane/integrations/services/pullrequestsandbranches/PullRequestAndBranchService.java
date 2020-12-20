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

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.dto.entities.Entity;
import com.hp.octane.integrations.dto.scm.Branch;
import com.hp.octane.integrations.dto.scm.PullRequest;
import com.hp.octane.integrations.services.entities.EntitiesService;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.BranchFetchParameters;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.CommitUserIdPicker;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.FetchHandler;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.PullRequestFetchParameters;
import com.hp.octane.integrations.services.rest.RestService;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;


public interface PullRequestAndBranchService {

    /**
     * Service instance producer - for internal usage only (protected by inaccessible configurer)
     *
     * @param configurer  SDK services configurer object
     * @param restService Rest Service
     * @param entitiesService
     * @return initialized service
     */
    static PullRequestAndBranchService newInstance(OctaneSDK.SDKServicesConfigurer configurer, RestService restService, EntitiesService entitiesService) {
        return new PullRequestAndBranchServiceImpl(configurer, restService, entitiesService);
    }

    void sendPullRequests(List<PullRequest> pullRequests, String workspaceId, PullRequestFetchParameters pullRequestFetchParameters, Consumer<String> logConsumer) throws IOException;

    long getPullRequestLastUpdateTime(String workspaceId, String repoUrl);

    BranchSyncResult syncBranchesToOctane(FetchHandler fetcherHandler, BranchFetchParameters fp, Long workspaceId, CommitUserIdPicker idPicker, Consumer<String> logConsumer) throws IOException;
}
