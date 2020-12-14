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

import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;

public class GithubServerFetchHandler extends GithubV3FetchHandler {

    public GithubServerFetchHandler(AuthenticationStrategy authenticationStrategy) {
        super(authenticationStrategy);
    }

    @Override
    public String getRepoApiPath(String repoHttpCloneUrl) {
        validateHttpCloneUrl(repoHttpCloneUrl);

        if(repoHttpCloneUrl.toLowerCase().startsWith(CLOUD_SERVICE_PREFIX)){
            throw new IllegalArgumentException("Supplied repository URL : " + repoHttpCloneUrl + " is Git Cloud URL. Change 'SCM Tool type' to Github Cloud.");
        }
        //   https://github.houston.softwaregrp.net:443/MQM/mqm.git;
        // =>https://github.houston.softwaregrp.net/api/v3/MQM/mqm
        int repoSlashIndex = repoHttpCloneUrl.lastIndexOf("/");
        int teamSlashIndex = repoHttpCloneUrl.substring(0, repoSlashIndex).lastIndexOf("/");
        String result = repoHttpCloneUrl.substring(0, teamSlashIndex) + "/api/v3/repos" + repoHttpCloneUrl.substring(teamSlashIndex);
        result = result.substring(0, result.length() - ".git".length());
        return result;
    }
}
