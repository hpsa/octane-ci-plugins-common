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

package com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.Entity;
import com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.EntityCollection;
import com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.ErrorDetails;
import com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.RequestErrors;

import java.util.stream.Collectors;

public class JsonConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T extends Entity> EntityCollection<T> convertCollection(String str, Class<T> entityType) throws JsonProcessingException {
        JavaType type = objectMapper.getTypeFactory().constructParametricType(EntityCollection.class, entityType);

        return objectMapper.readValue(str, type);
    }

    public static String getErrorMessage(String jsonString) {
        try {
            RequestErrors requestErrors = objectMapper.readValue(jsonString, RequestErrors.class);
            if (!requestErrors.getErrors().isEmpty()) {
                return requestErrors.getErrors().stream().map(ErrorDetails::getMessage).collect(Collectors.joining("; "));
            }
        } catch (JsonProcessingException e) {
            //do nothing
        }
        return jsonString;
    }
}
