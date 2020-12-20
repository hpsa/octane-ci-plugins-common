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

package com.hp.octane.integrations.services.pullrequestsandbranches.github;


import com.hp.octane.integrations.dto.scm.PullRequest;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.BranchFetchParameters;
import com.hp.octane.integrations.services.pullrequestsandbranches.factory.CommitUserIdPicker;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class GithubCloudFetchHandler extends GithubV3FetchHandler {

    public GithubCloudFetchHandler(AuthenticationStrategy authenticationStrategy) {
        super(authenticationStrategy);
    }

    @Override
    public String getRepoApiPath(String repoHttpCloneUrl) {
        validateHttpCloneUrl(repoHttpCloneUrl);


        //"  https://github.com/jenkinsci/hpe-application-automation-tools-plugin.git";
        // =>https://api.github.com/repos/jenkinsci/hpe-application-automation-tools-plugin
        if(!repoHttpCloneUrl.toLowerCase().startsWith(CLOUD_SERVICE_PREFIX)){
            throw new IllegalArgumentException("Unexpected github cloud repository URL : " + repoHttpCloneUrl + ". Git Cloud URL must start with : https://github.com/. ");
        }
        List<String> parts = Arrays.asList(repoHttpCloneUrl.trim().substring(CLOUD_SERVICE_PREFIX.length()).split("/"));
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Unexpected github cloud repository URL : " + repoHttpCloneUrl + ". Expected format : https://github.com/<user_name>/<repo_name>.git");
        }

        String user = parts.get(parts.size() - 2);
        String repoName = parts.get(parts.size() - 1);
        repoName = repoName.substring(0, repoName.length() - ".git".length());
        return String.format("https://api.github.com/repos/%s/%s", user, repoName);
    }

    @Override
    protected String getApiPath(String repoHttpCloneUrl) {
        return "https://api.github.com";
    }
}
