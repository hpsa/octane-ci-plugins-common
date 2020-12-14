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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.RequestError;

import java.util.List;

public class JsonConverter {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> List<T> convertCollection(String str, Class<T> entityType) throws JsonProcessingException {
        JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, entityType);
        return objectMapper.readValue(str, type);
    }

    public static <T> T convert(String str, Class<T> entityType) throws JsonProcessingException {
        return objectMapper.readValue(str, entityType);
    }

    public static String getErrorMessage(String jsonString) {
        try {
            RequestError requestError = objectMapper.readValue(jsonString, RequestError.class);
            return requestError.getMessage() + (requestError.getDocumentation_url() != null ? ", See documentation " + requestError.getDocumentation_url() : "");

        } catch (JsonProcessingException e) {
            //do nothing
        }
        return jsonString;
    }
}
