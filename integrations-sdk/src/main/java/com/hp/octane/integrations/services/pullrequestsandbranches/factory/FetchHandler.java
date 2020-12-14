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

import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.scm.PullRequest;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.GeneralRestClient;
import com.hp.octane.integrations.services.pullrequestsandbranches.rest.authentication.AuthenticationStrategy;
import org.apache.http.HttpStatus;
import org.apache.http.conn.HttpHostConnectException;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public abstract class FetchHandler {

    protected static final DTOFactory dtoFactory = DTOFactory.getInstance();
    protected final GeneralRestClient restClient;

    public FetchHandler(AuthenticationStrategy authenticationStrategy) {
        restClient = new GeneralRestClient(authenticationStrategy);
    }

    public abstract List<PullRequest> fetchPullRequests(PullRequestFetchParameters parameters, CommitUserIdPicker commitUserIdPicker, Consumer<String> logger) throws IOException;

    protected abstract String getRepoApiPath(String clonePath);

    protected abstract String getApiPath(String clonePath);

    protected abstract String parseRequestError(OctaneResponse response);

    public void pingRepository(String repoApiBaseUrl, Consumer<String> logConsumer) throws IOException {
        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class).setUrl(repoApiBaseUrl).setMethod(HttpMethod.GET);
        try {
            OctaneResponse response = restClient.executeRequest(request);
            if (response.getStatus() == HttpStatus.SC_OK) {
                logConsumer.accept("Ping repository : Ok");
                return;
            }
            logConsumer.accept("Ping repository : " + response.getStatus() + "; " + parseRequestError(response));
            if (response.getStatus() == HttpStatus.SC_NOT_FOUND) {
                throw new IllegalArgumentException("Repository not found. Please validate that project/user name and repository name are spelled correctly.");
            }
            if (response.getStatus() == HttpStatus.SC_UNAUTHORIZED) {
                throw new IllegalArgumentException("Unauthorized, validate correctness of credentials.");
            }

            throw new IllegalArgumentException("Unexpected exception");

        } catch (HttpHostConnectException e) {
            throw new IOException("Repository is not available. Please validate that URL and proxy settings are set correctly.");
        }
    }

    protected void validateHttpCloneUrl(String clonePath) {
        if (clonePath == null || clonePath.isEmpty()) {
            throw new IllegalArgumentException("Repo url cannot be empty");
        }

        String clonePathLowerCase = clonePath.toLowerCase();
        String repoSuffix = ".git";

        if (clonePathLowerCase.startsWith("ssh")) {
            throw new IllegalArgumentException("SSH protocol is not supported by this action.");
        }
        if (!clonePath.toLowerCase().startsWith("http")) {
            throw new IllegalArgumentException("Repo url must start with 'http(s)'");
        }
        if (!clonePathLowerCase.endsWith(repoSuffix)) {
            throw new IllegalArgumentException("Repo url must end with '.git'");
        }
    }


    public static String getUserName(CommitUserIdPicker idPicker, String email, String name) {
        if (idPicker != null) {
            return idPicker.getUserIdForCommit(email, name);
        }
        return getUserName(email, name);
    }

    public static String getUserName(String email, String name) {
        if (email != null && email.contains("@")) {
            String[] emailParts = email.split("@");
            if (emailParts.length > 1) {
                String id = emailParts[0].trim();
                if (!id.isEmpty()) {
                    return id;
                }
            }
        }

        return name;
    }
}
