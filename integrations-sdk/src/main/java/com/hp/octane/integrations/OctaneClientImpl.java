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

package com.hp.octane.integrations;

import com.hp.octane.integrations.dto.general.OctaneConnectivityStatus;
import com.hp.octane.integrations.exceptions.OctaneConnectivityException;
import com.hp.octane.integrations.exceptions.OctaneValidationException;
import com.hp.octane.integrations.services.bridge.BridgeService;
import com.hp.octane.integrations.services.configuration.ConfigurationService;
import com.hp.octane.integrations.services.coverage.CoverageService;
import com.hp.octane.integrations.services.entities.EntitiesService;
import com.hp.octane.integrations.services.events.EventsService;
import com.hp.octane.integrations.services.logging.LoggingService;
import com.hp.octane.integrations.services.logs.LogsService;
import com.hp.octane.integrations.services.pipelines.PipelineContextService;
import com.hp.octane.integrations.services.pullrequestsandbranches.PullRequestAndBranchService;
import com.hp.octane.integrations.services.queueing.QueueingService;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.services.scmdata.SCMDataService;
import com.hp.octane.integrations.services.sonar.SonarService;
import com.hp.octane.integrations.services.tasking.TasksProcessor;
import com.hp.octane.integrations.services.tests.TestsService;
import com.hp.octane.integrations.services.vulnerabilities.VulnerabilitiesService;
import com.hp.octane.integrations.services.vulnerabilities.VulnerabilitiesToolService;
import com.hp.octane.integrations.services.vulnerabilities.fod.FODService;
import com.hp.octane.integrations.services.vulnerabilities.sonar.SonarVulnerabilitiesService;
import com.hp.octane.integrations.services.vulnerabilities.ssc.SSCService;
import com.hp.octane.integrations.utils.CIPluginSDKUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * protected implementation of the OctaneClient
 * for internal usage only
 * refer to OctaneClient API definition for a function specification
 */

final class OctaneClientImpl implements OctaneClient {
    private static final Logger logger = LogManager.getLogger(OctaneClientImpl.class);

    private final OctaneSDK.SDKServicesConfigurer configurer;
    private final LoggingService loggingService;
    private final BridgeService bridgeService;
    private final ConfigurationService configurationService;
    private final CoverageService coverageService;
    private final SonarService sonarService;
    private final SSCService sscService;
    private final EntitiesService entitiesService;
    private final SonarVulnerabilitiesService sonarVulnerabilitiesService;

    private final PipelineContextService pipelineContextService;
    private final EventsService eventsService;
    private final LogsService logsService;
    private final QueueingService queueingService;
    private final RestService restService;
    private final TasksProcessor tasksProcessor;
    private final TestsService testsService;
    private final VulnerabilitiesService vulnerabilitiesService;
    private final PullRequestAndBranchService pullRequestAndBranchService;
    private final SCMDataService scmDataService;
    private final Thread shutdownHook;
    private boolean isShutdownHookActivated;
    private long shutdownHookActivatedTime;
    private long started = System.currentTimeMillis();

    OctaneClientImpl(OctaneSDK.SDKServicesConfigurer configurer) {
        if (configurer == null) {
            throw new IllegalArgumentException("services configurer MUST NOT be null nor empty");
        }

        //  internals init
        this.configurer = configurer;
        ensureStorageIfAny();
        loggingService = LoggingService.newInstance(configurer);
        queueingService = QueueingService.newInstance(configurer);

        //sdk validation services
        restService = RestService.newInstance(configurer);
        configurationService = ConfigurationService.newInstance(configurer, restService);

        if (configurer.octaneConfiguration.isSuspended()) {
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "Client is SUSPENDED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }

        refreshSdkSupported();
        if (!configurer.octaneConfiguration.isSdkSupported()) {
            logger.error(configurer.octaneConfiguration.geLocationForLog() + "Client is DISABLED: " + OctaneConnectivityException.UNSUPPORTED_SDK_VERSION_MESSAGE);
        }

        //  independent services init
        tasksProcessor = TasksProcessor.newInstance(configurer, configurationService);
        tasksProcessor.resetJobListCache();

        //  dependent services init

        coverageService = CoverageService.newInstance(configurer, queueingService, restService, configurationService);
        entitiesService = EntitiesService.newInstance(configurer, restService);
        pipelineContextService = PipelineContextService.newInstance(configurer, restService);
        eventsService = EventsService.newInstance(configurer, restService, configurationService);
        logsService = LogsService.newInstance(configurer, queueingService, restService, configurationService);
        testsService = TestsService.newInstance(configurer, queueingService, restService, configurationService);

        sscService = SSCService.newInstance(configurer, restService);
        sonarService = SonarService.newInstance(configurer, queueingService, coverageService, configurationService);
        sonarVulnerabilitiesService = SonarVulnerabilitiesService.newInstance(configurer, restService);
        FODService fodService = FODService.newInstance(configurer, restService);

        VulnerabilitiesToolService[] vulnerabilitiesToolServices = {sscService, sonarVulnerabilitiesService, fodService};
        vulnerabilitiesService = VulnerabilitiesService.newInstance(queueingService, vulnerabilitiesToolServices, configurer, restService, configurationService);

        pullRequestAndBranchService = PullRequestAndBranchService.newInstance(configurer, restService, entitiesService);

        scmDataService = SCMDataService.newInstance(queueingService, configurer, restService, configurationService, eventsService);

        //  bridge init is the last one, to make sure we are not processing any task until all services are up
        bridgeService = BridgeService.newInstance(configurer, restService, tasksProcessor, configurationService);

        //  register shutdown hook to allow graceful shutdown of services/resources
        shutdownHook = new Thread(() -> {
            String instanceId = configurer.octaneConfiguration.getInstanceId();
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "closing OctaneClient " + instanceId + " as per Runtime shutdown request...");
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            try {
                this.isShutdownHookActivated = true;
                this.shutdownHookActivatedTime = System.currentTimeMillis();
                this.close();
            } catch (Throwable throwable) {
                logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed during shutdown of OctaneClient " + instanceId, throwable);
            } finally {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "...OctaneClient " + instanceId + " CLOSED");
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        configurer.octaneConfiguration.getParameterNames().forEach(paramName -> {
            logger.info(configurer.octaneConfiguration.geLocationForLog() + String.format("System parameter %s:%s", paramName,
                    configurer.octaneConfiguration.getParameter(paramName).getRawValue()));
        });

        logger.info(configurer.octaneConfiguration.geLocationForLog() + "OctaneClient initialized with instance ID: " + configurer.octaneConfiguration.getInstanceId());
    }

    @Override
    public void refreshSdkSupported() {
        OctaneConnectivityStatus octaneConnectivityStatus = configurationService.getOctaneConnectivityStatus();
        if (octaneConnectivityStatus != null) {
            configurer.octaneConfiguration.setSdkSupported(CIPluginSDKUtils.isSdkSupported(octaneConnectivityStatus));
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "sdkSupported = " + configurer.octaneConfiguration.isSdkSupported());
        } else {
            logger.info(configurer.octaneConfiguration.geLocationForLog() + "refreshSdkSupported : octaneConnectivityStatus==null");
        }
    }

    @Override
    public String getInstanceId() {
        return configurer.octaneConfiguration.getInstanceId();
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return configurationService;
    }

    @Override
    public CoverageService getCoverageService() {
        return coverageService;
    }

    @Override
    public SonarService getSonarService() {
        return sonarService;
    }

    @Override
    public EntitiesService getEntitiesService() {
        return entitiesService;
    }

    @Override
    public BridgeService getBridgeService() {
        return bridgeService;
    }

    @Override
    public PipelineContextService getPipelineContextService() {
        return pipelineContextService;
    }

    @Override
    public EventsService getEventsService() {
        return eventsService;
    }

    @Override
    public LogsService getLogsService() {
        return logsService;
    }

    @Override
    public RestService getRestService() {
        return restService;
    }

    @Override
    public TasksProcessor getTasksProcessor() {
        return tasksProcessor;
    }

    @Override
    public TestsService getTestsService() {
        return testsService;
    }

    @Override
    public PullRequestAndBranchService getPullRequestAndBranchService() {
        return pullRequestAndBranchService;
    }


    @Override
    public VulnerabilitiesService getVulnerabilitiesService() {
        return vulnerabilitiesService;
    }

    @Override
    public SCMDataService getSCMDataService() {
        return scmDataService;
    }

    @Override
    public void validateOctaneIsActiveAndSupportVersion(String version) {
        if (!this.getConfigurationService().isConnected()) {
            throw new OctaneValidationException("ALM Octane is not connected.");
        }
        if (configurer.octaneConfiguration.isSuspended()) {
            throw new OctaneValidationException("ALM Octane is suspended.");
        }
        if (!this.getConfigurationService().isOctaneVersionGreaterOrEqual(version)) {
            throw new OctaneValidationException(String.format("Required ALM Octane version is %s, but connected ALM Octane has lower version %s.", version,
                    this.getConfigurationService().getOctaneConnectivityStatus().getOctaneVersion()));
        }
    }

    @Override
    public String toString() {
        return "OctaneClientImpl{ instanceId: " + configurer.octaneConfiguration.getInstanceId() + " }";
    }

    /**
     * private API to cleanly close the OctaneClient with all its resources
     * use-cases: JVM shutdown, temporary client suspension, complete client removal
     */
    private void close() {
        queueingService.shutdown();
        bridgeService.shutdown();
        coverageService.shutdown();
        sonarService.shutdown();
        eventsService.shutdown();
        logsService.shutdown();
        testsService.shutdown();
        vulnerabilitiesService.shutdown();
        restService.obtainOctaneRestClient().shutdown();
        loggingService.shutdown();
        scmDataService.shutdown();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    /**
     * package-protected API to completely remove OctaneClient: closing its services, releasing the resources, deleting any persisted artifacts
     * use-cases: OctaneClient configuration being removed from consumer's application
     */
    void remove() {
        logger.info(configurer.octaneConfiguration.geLocationForLog() + "Removing client");
        //  shut down services
        close();

        logger.info(configurer.octaneConfiguration.geLocationForLog() + "Removing client - services closed");

        //  clean storage
        if (configurer.pluginServices.getAllowedOctaneStorage() != null) {
            String instanceId = configurer.octaneConfiguration.getInstanceId();
            File instanceOrientedStorage = new File(configurer.pluginServices.getAllowedOctaneStorage(), "nga" + File.separator + instanceId);
            if (deleteFolder(instanceOrientedStorage)) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "cleaned dedicated storage");
            } else {
                logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed to clean dedicated storage");
            }
        }
        logger.info(configurer.octaneConfiguration.geLocationForLog() + "Removing client done");
    }

    private void ensureStorageIfAny() {
        if (configurer.pluginServices.getAllowedOctaneStorage() != null) {
            String instanceId = configurer.octaneConfiguration.getInstanceId();
            File instanceOrientedStorage = new File(configurer.pluginServices.getAllowedOctaneStorage(), "nga" + File.separator + instanceId);
            if (instanceOrientedStorage.exists()) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "dedicated storage is exist for instance " + configurer.octaneConfiguration.getInstanceId());
            } else if (instanceOrientedStorage.mkdirs()) {
                logger.info(configurer.octaneConfiguration.geLocationForLog() + "dedicated storage is created for instance " + configurer.octaneConfiguration.getInstanceId());
            } else {
                logger.error(configurer.octaneConfiguration.geLocationForLog() + "failed to create dedicated storage : " + instanceOrientedStorage.getAbsolutePath());
            }
        }
    }

    private boolean deleteFolder(File folder) {
        File[] children = folder.listFiles();
        if (children != null) {
            for (File file : children) {
                deleteFolder(file);
            }
        }
        return folder.delete();
    }

    void notifyCredentialsChanged() {
        restService.notifyConfigurationChange();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("location", configurer.octaneConfiguration.geLocationForLog());
        map.put("instanceId", configurer.octaneConfiguration.getInstanceId());
        map.put("sdkSupported", configurer.octaneConfiguration.isSdkSupported());
        map.put("isDisabled", configurer.octaneConfiguration.isDisabled());
        map.put("shutdownHookActivated", this.isShutdownHookActivated);
        if (isShutdownHookActivated) {
            map.put("shutdownHookActivatedTime", new Date(shutdownHookActivatedTime));
        }
        map.put("isConnected", this.getConfigurationService().isConnected());
        OctaneConnectivityStatus status = this.getConfigurationService().getOctaneConnectivityStatus();
        if (status != null) {
            map.put("octaneVersion", status.getOctaneVersion());
            map.put("supportedSdkVersion", status.getSupportedSdkVersion());
        }
        map.put("started", new Date(started));
        return map;
    }
}
