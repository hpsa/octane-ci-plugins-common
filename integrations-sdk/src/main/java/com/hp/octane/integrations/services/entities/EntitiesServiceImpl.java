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

package com.hp.octane.integrations.services.entities;

import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.entities.*;
import com.hp.octane.integrations.exceptions.OctaneBulkException;
import com.hp.octane.integrations.exceptions.OctaneRestException;
import com.hp.octane.integrations.services.rest.OctaneRestClient;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.utils.SdkStringUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Pattern;

import static com.hp.octane.integrations.services.rest.RestService.ACCEPT_HEADER;
import static com.hp.octane.integrations.services.rest.RestService.CONTENT_TYPE_HEADER;

/**
 * Default implementation of tests service
 */

final class EntitiesServiceImpl implements EntitiesService {
    private static final Logger logger = LogManager.getLogger(EntitiesServiceImpl.class);
    private static final DTOFactory dtoFactory = DTOFactory.getInstance();

    private final OctaneSDK.SDKServicesConfigurer configurer;
    private final RestService restService;

    private static final String FILTERING_FRAGMENT = "query={query}";
    private static final String FIELDS_FRAGMENT = "fields={fields}";
    private static final String PAGING_FRAGMENT = "offset={offset}&limit={limit}";
    private static final String ORDER_BY_FRAGMENT = "order_by={order}";
    private static final String URI_PARAM_ENCODING = "UTF-8";
    private static final int MAX_GET_LIMIT = 1000;
	private static final int MAX_UPDATE_LIMIT = 200;

    EntitiesServiceImpl(OctaneSDK.SDKServicesConfigurer configurer, RestService restService) {
        if (configurer == null) {
            throw new IllegalArgumentException("invalid configurer");
        }
        if (restService == null) {
            throw new IllegalArgumentException("rest service MUST NOT be null");
        }
        this.configurer = configurer;
        this.restService = restService;
        logger.info(configurer.octaneConfiguration.geLocationForLog() + "initialized SUCCESSFULLY");
    }

    @Override
    public List<Entity> deleteEntitiesByIds(Long workspaceId, String entityCollectionName, Collection<?> entitiesIds) {

        //CREATE DELETE CONDITION
        if (entitiesIds == null || entitiesIds.isEmpty()) {
            return null;
        }
        String deleteCondition = QueryHelper.conditionIn("id", entitiesIds, true);
        return deleteEntities(workspaceId, entityCollectionName, Collections.singletonList(deleteCondition));
    }

    @Override
    public List<Entity> deleteEntities(Long workspaceId, String entityCollectionName, Collection<String> conditions) {
        //SEND REQUEST
        OctaneRestClient octaneRestClient = restService.obtainOctaneRestClient();
        Map<String, String> headers = new HashMap<>();
        headers.put(ACCEPT_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(OctaneRestClient.CLIENT_TYPE_HEADER, OctaneRestClient.CLIENT_TYPE_VALUE);
        String url = buildEntityUrl(workspaceId, entityCollectionName, conditions, null, null, null, null);
        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
                .setMethod(HttpMethod.DELETE)
                .setUrl(url)
                .setHeaders(headers);
        OctaneResponse response = executeRequest(octaneRestClient, request);
        ResponseEntityList responseEntityList = parseBody(HttpStatus.SC_OK, response);
        return responseEntityList.getData();
    }

    @Override
    public List<Entity> updateEntities(Long workspaceId, String entityCollectionName, List<Entity> entities) {
        List<Entity> results = new ArrayList<>();
        ListUtils.partition(entities, MAX_UPDATE_LIMIT).forEach(list -> {
            EntityList entitiesForUpdate = dtoFactory.newDTO(EntityList.class);
            entitiesForUpdate.setData(entities);
            String jsonData = dtoFactory.dtoToJson(entitiesForUpdate);
            List<Entity> temp = updateEntities(workspaceId, entityCollectionName, jsonData);
            results.addAll(temp);
        });
        return results;
    }

    @Override
    public List<Entity> updateEntities(Long workspaceId, String entityCollectionName, String jsonData) {
        OctaneRestClient octaneRestClient = restService.obtainOctaneRestClient();
        Map<String, String> headers = new HashMap<>();
        headers.put(ACCEPT_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(CONTENT_TYPE_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(OctaneRestClient.CLIENT_TYPE_HEADER, OctaneRestClient.CLIENT_TYPE_VALUE);

        String url = buildEntityUrl(workspaceId, entityCollectionName, null, null, null, null, null);

        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
                .setMethod(HttpMethod.PUT)
                .setUrl(url)
                .setBody(jsonData)
                .setHeaders(headers);
        OctaneResponse response = executeRequest(octaneRestClient, request);
        ResponseEntityList responseEntityList = parseBody(HttpStatus.SC_OK, response);
        return responseEntityList.getData();
    }

    @Override
    public List<Entity> postEntities(Long workspaceId, String entityCollectionName, List<Entity> entities) {
		List<Entity> results = new ArrayList<>();
    	ListUtils.partition(entities, MAX_UPDATE_LIMIT).forEach(list -> {
			EntityList entitiesForUpdate = dtoFactory.newDTO(EntityList.class);
			entitiesForUpdate.setData(entities);
			String jsonData = dtoFactory.dtoToJson(entitiesForUpdate);
			List<Entity> temp = postEntities(workspaceId, entityCollectionName, jsonData);
			results.addAll(temp);
		});
		return results;
    }

    @Override
    public List<Entity> postEntities(Long workspaceId, String entityCollectionName, String jsonData) {
        OctaneRestClient octaneRestClient = restService.obtainOctaneRestClient();
        Map<String, String> headers = new HashMap<>();
        headers.put(ACCEPT_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(CONTENT_TYPE_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(OctaneRestClient.CLIENT_TYPE_HEADER, OctaneRestClient.CLIENT_TYPE_VALUE);

        String url = buildEntityUrl(workspaceId, entityCollectionName, null, null, null, null, null);
        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
                .setMethod(HttpMethod.POST)
                .setUrl(url)
                .setBody(jsonData)
                .setHeaders(headers);
        OctaneResponse response = executeRequest(octaneRestClient, request);
        ResponseEntityList responseEntityList = parseBody(HttpStatus.SC_CREATED, response);
        return responseEntityList.getData();
    }

    @Override
    public List<Entity> getEntities(Long workspaceId, String entityCollectionName, Collection<String> conditions, Collection<String> fields) {

        List<Entity> result = new ArrayList<>();
        int limit = MAX_GET_LIMIT;
        boolean fetchedAll = false;
        while (!fetchedAll) {
            String url = buildEntityUrl(workspaceId, entityCollectionName, conditions, fields, result.size(), limit, null);
            ResponseEntityList responseEntityList = getPagedEntities(url);
            result.addAll(responseEntityList.getData());
            fetchedAll = responseEntityList.getData().isEmpty() || responseEntityList.getTotalCount() == 0 || responseEntityList.getTotalCount() == result.size();
        }

        return result;
    }

    @Override
    public List<Entity> getEntitiesByIds(Long workspaceId, String collectionName, Collection<?> ids) {
        if (ids == null || ids.size() == 0) {
            return new LinkedList<>();
        }
        int limit = 100;
        if (ids.size() > limit) {
            throw new IllegalArgumentException("List of ids is too long. Only " + limit + " values are allowed.");
        }

        List<String> conditions = new LinkedList();
        conditions.add(QueryHelper.conditionIn(EntityConstants.Base.ID_FIELD, ids, true));

        String url = buildEntityUrl(workspaceId, collectionName, conditions, null, 0, limit, null);
        ResponseEntityList result = getPagedEntities(url);
        return result.getData();
    }

    @Override
    public ResponseEntityList getPagedEntities(String url) {
        OctaneRestClient octaneRestClient = restService.obtainOctaneRestClient();
        Map<String, String> headers = new HashMap<>();
        headers.put(ACCEPT_HEADER, ContentType.APPLICATION_JSON.getMimeType());
        headers.put(OctaneRestClient.CLIENT_TYPE_HEADER, OctaneRestClient.CLIENT_TYPE_VALUE);

        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
                .setMethod(HttpMethod.GET)
                .setUrl(url)
                .setHeaders(headers);
        OctaneResponse response = executeRequest(octaneRestClient, request);

        ResponseEntityList pagedEntityList = parseBody(HttpStatus.SC_OK, response);

        return pagedEntityList;
    }

    private OctaneResponse executeRequest(OctaneRestClient octaneRestClient, OctaneRequest request) {
        try {
            return octaneRestClient.execute(request);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static ResponseEntityList parseBody(int expectedResult, OctaneResponse response) {
        String body = response.getBody();
        if (response.getStatus() == expectedResult) {
            return dtoFactory.dtoFromJson(body, ResponseEntityList.class);
        } else {
            try {
                if (body.contains("exceeds_total_count")) {
                    OctaneBulkExceptionData data = dtoFactory.dtoFromJson(body, OctaneBulkExceptionData.class);
                    throw new OctaneBulkException(response.getStatus(), data);
                } else {
                    OctaneRestExceptionData data = dtoFactory.dtoFromJson(body, OctaneRestExceptionData.class);
                    throw new OctaneRestException(response.getStatus(), data);
                }
            } catch (OctaneRestException | OctaneBulkException ex1) {
                throw ex1;
            } catch (Exception ex2) {
                throw new RuntimeException("The request failed : " + body, ex2);
            }
        }
    }

    public String buildEntityUrl(Long workspaceId, String collection, Collection<String> conditions, Collection<String> fields, Integer offset, Integer limit, String orderBy) {

        StringBuilder template = new StringBuilder(configurer.octaneConfiguration.getUrl());
        template.append("/api/shared_spaces/");
        template.append(configurer.octaneConfiguration.getSharedSpace());
        if (workspaceId != null) {
            template.append("/workspaces/");
            template.append(workspaceId);
        }

        template.append("/");
        template.append(collection);
        template.append("?");

        Map<String, Object> params = new HashMap<>();
        if (offset != null && limit != null) {
            params.put("offset", offset);
            params.put("limit", limit);
            template.append("&" + PAGING_FRAGMENT);
        }

        if (conditions != null && !conditions.isEmpty()) {
            StringBuilder expr = new StringBuilder();
            for (String condition : conditions) {
                if (expr.length() > 0) {
                    expr.append(";");
                }
                expr.append(condition);
            }
            params.put("query", "\"" + expr.toString() + "\"");
            template.append("&" + FILTERING_FRAGMENT);
        }

        if (fields != null && !fields.isEmpty()) {
            params.put("fields", SdkStringUtils.join(fields, ","));
            template.append("&" + FIELDS_FRAGMENT);
        }

        if (SdkStringUtils.isNotEmpty(orderBy)) {
            params.put("order", orderBy);
            template.append("&" + ORDER_BY_FRAGMENT);
        }
        return resolveTemplate(template.toString(), params);
    }

    private static String resolveTemplate(String template, Map<String, ?> params) {
        String result = template;
        for (Map.Entry<String, ?> param : params.entrySet()) {
            result = result.replaceAll(Pattern.quote("{" + param.getKey() + "}"), encodeParam(param.getValue() == null ? "" : param.getValue().toString()));
        }
        return result;
    }

    private static String encodeParam(String param) {
        try {
            return URLEncoder.encode(param, URI_PARAM_ENCODING).replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unsupported encoding used for URI parameter encoding.", e);
        }
    }

}
