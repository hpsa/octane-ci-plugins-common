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

package com.hp.octane.integrations.services.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.octane.integrations.OctaneConfiguration;
import com.hp.octane.integrations.OctaneSDK;
import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.connectivity.HttpMethod;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.dto.general.OctaneConnectivityStatus;
import com.hp.octane.integrations.exceptions.OctaneConnectivityException;
import com.hp.octane.integrations.services.configurationparameters.factory.ConfigurationParameterFactory;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.utils.CIPluginSDKUtils;
import com.hp.octane.integrations.utils.SdkConstants;
import com.hp.octane.integrations.utils.SdkStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Base implementation of Configuration Service API
 */

public final class ConfigurationServiceImpl implements ConfigurationService {
    private static final Logger logger = LogManager.getLogger(ConfigurationServiceImpl.class);
    private static final DTOFactory dtoFactory = DTOFactory.getInstance();

    private static final String CONNECTIVITY_STATUS_URL = "/analytics/ci/servers/connectivity/status";

    private static final String PIPELINE_ROOTS_URL = "/analytics/ci/servers/%s/pipeline-roots";//{%s - ciServerIdentity}
    private static final String OCTANE_ROOTS_VERSION = "15.1.8";
    private Set<String> octaneRoots = null;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final OctaneSDK.SDKServicesConfigurer configurer;
    private final RestService restService;
    private OctaneConnectivityStatus octaneConnectivityStatus;
    private volatile boolean isConnected;
    private ExecutorService octaneRootsCacheExecutor = Executors.newSingleThreadExecutor();

    ConfigurationServiceImpl(OctaneSDK.SDKServicesConfigurer configurer, RestService restService) {
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
    public OctaneConfiguration getConfiguration() {
        return configurer.octaneConfiguration;
    }

    @Override
    public synchronized OctaneConnectivityStatus getOctaneConnectivityStatus() {
        return getOctaneConnectivityStatus(false);
    }

    public synchronized OctaneConnectivityStatus getOctaneConnectivityStatus(boolean forceFetch) {

        try {
            if (forceFetch || octaneConnectivityStatus == null) {
                octaneConnectivityStatus = validateConfigurationAndGetConnectivityStatus();
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "octaneConnectivityStatus : " + octaneConnectivityStatus);
                isConnected = true;
                resetOctaneRootsCache();
            }
        } catch (Exception e) {
            logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed to getOctaneConnectivityStatus : " + e.getMessage());
        }

        return octaneConnectivityStatus;
    }

    @Override
    public OctaneConnectivityStatus validateConfigurationAndGetConnectivityStatus() throws IOException {
        OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class)
                .setMethod(HttpMethod.GET)
                .setUrl(configurer.octaneConfiguration.getUrl() + RestService.SHARED_SPACE_INTERNAL_API_PATH_PART + configurer.octaneConfiguration.getSharedSpace() + CONNECTIVITY_STATUS_URL);

        OctaneResponse response = restService.obtainOctaneRestClient().execute(request, configurer.octaneConfiguration);
        if (response.getStatus() == 401) {
            throw new OctaneConnectivityException(response.getStatus(), OctaneConnectivityException.AUTHENTICATION_FAILURE_KEY, OctaneConnectivityException.AUTHENTICATION_FAILURE_MESSAGE);
        } else if (response.getStatus() == 403) {
            throw new OctaneConnectivityException(response.getStatus(), OctaneConnectivityException.AUTHORIZATION_FAILURE_KEY, OctaneConnectivityException.AUTHORIZATION_FAILURE_MESSAGE);
        } else if (response.getStatus() == 404) {
            throw new OctaneConnectivityException(response.getStatus(), OctaneConnectivityException.CONN_SHARED_SPACE_INVALID_KEY, OctaneConnectivityException.CONN_SHARED_SPACE_INVALID_MESSAGE);
        } else if (response.getStatus() == 200) {
            OctaneConnectivityStatus octaneConnectivityStatus = DTOFactory.getInstance().dtoFromJson(response.getBody(), OctaneConnectivityStatus.class);
            return octaneConnectivityStatus;
        } else {
            throw new OctaneConnectivityException(response.getStatus(), OctaneConnectivityException.UNEXPECTED_FAILURE_KEY, OctaneConnectivityException.UNEXPECTED_FAILURE_MESSAGE + ": " + response.getStatus());
        }
    }

    @Override
    public boolean isOctaneVersionGreaterOrEqual(String version) {
        OctaneConnectivityStatus octaneStatus = getOctaneConnectivityStatus();
        return (octaneStatus != null && octaneStatus.getOctaneVersion() != null && CIPluginSDKUtils.compareStringVersion(octaneStatus.getOctaneVersion(), version) >= 0);
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    @Override
    public Collection<String> getOctaneRootsCacheCollection() {
        if (octaneRoots == null) {
            return Collections.EMPTY_SET;
        }
        return Collections.unmodifiableCollection(octaneRoots.stream().sorted().collect(Collectors.toList()));
    }

    @Override
    public Future<Boolean> resetOctaneRootsCache() {
        if (isOctaneRootsCacheActivated() && isOctaneVersionGreaterOrEqual(OCTANE_ROOTS_VERSION) && !configurer.octaneConfiguration.isDisabled()) {
            return octaneRootsCacheExecutor.submit(() -> {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "resetOctaneRootCache started");
                try {
                    long startTime = System.currentTimeMillis();
                    String url = configurer.octaneConfiguration.getUrl() + RestService.SHARED_SPACE_INTERNAL_API_PATH_PART +
                            configurer.octaneConfiguration.getSharedSpace() + String.format(PIPELINE_ROOTS_URL, configurer.octaneConfiguration.getInstanceId());
                    OctaneRequest request = dtoFactory.newDTO(OctaneRequest.class).setMethod(HttpMethod.GET).setUrl(url);

                    OctaneResponse response = restService.obtainOctaneRestClient().execute(request, configurer.octaneConfiguration);
                    octaneRoots = mapper.readValue(response.getBody(), mapper.getTypeFactory().constructCollectionType(Set.class, String.class));
                    logger.info(configurer.octaneConfiguration.geLocationForLog() + "resetOctaneRootCache: successfully update octane roots, found " +
                            octaneRoots.size() + " roots, processing time is " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
                    return true;
                } catch (Exception e) {
                    logger.info(configurer.octaneConfiguration.geLocationForLog() + "Failed to resetOctaneRootCache : " + e.getMessage());
                    return false;
                }
            });
        } else {
            if (octaneRoots != null) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "resetOctaneRootsCache : cache is cleared");
            }
            octaneRoots = null;
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public void addToOctaneRootsCache(String rootJob) {
        if (octaneRoots != null && SdkStringUtils.isNotEmpty(rootJob)) {
            if (octaneRoots.add(rootJob)) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "addToOctaneRootsCache: new root is added [" + rootJob + "]");
            }
        }
    }

    @Override
    public boolean removeFromOctaneRoots(String rootJob) {
        if (octaneRoots != null) {
            return octaneRoots.remove(rootJob);
        }
        return false;
    }

    public boolean isRelevantForOctane(String rootJobs) {
        if (rootJobs == null) {
            return true;
        }
        Collection<String> parents;
        if (rootJobs.contains(SdkConstants.General.JOB_PARENT_DELIMITER)) {
            parents = Arrays.asList(rootJobs.split(SdkConstants.General.JOB_PARENT_DELIMITER));
        } else {
            parents = Collections.singleton(rootJobs);
        }
        return isRelevantForOctane(parents);
    }

    @Override
    public boolean isRelevantForOctane(Collection<String> rootJobs) {
        if (isOctaneRootsCacheActivated() && octaneRoots != null && rootJobs != null && !rootJobs.isEmpty()) {
            for (String rootJob : rootJobs) {
                if (SdkStringUtils.isEmpty(rootJob)) {
                    continue;
                }
                if (octaneRoots.contains(rootJob)) {
                    return true;
                }
                //multibranch handling
                String parentJobName = configurer.pluginServices.getParentJobName(rootJob);
                if (parentJobName != null && octaneRoots.contains(parentJobName)) {
                    addToOctaneRootsCache(rootJob);
                    return true;
                }

            }
            return false;
        }
        return true;
    }

    private boolean isOctaneRootsCacheActivated() {
        return ConfigurationParameterFactory.octaneRootsCacheAllowed(configurer.octaneConfiguration);
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("isOctaneRootsCacheActivated", isOctaneRootsCacheActivated());
        if (isOctaneRootsCacheActivated() && octaneRoots != null) {
            map.put("octaneRootsCache_jobCount", octaneRoots.size());
        }
        return map;
    }
}
