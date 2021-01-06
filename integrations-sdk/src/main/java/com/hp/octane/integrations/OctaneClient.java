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

import com.hp.octane.integrations.services.HasMetrics;
import com.hp.octane.integrations.services.bridge.BridgeService;
import com.hp.octane.integrations.services.configuration.ConfigurationService;
import com.hp.octane.integrations.services.coverage.CoverageService;
import com.hp.octane.integrations.services.pullrequestsandbranches.PullRequestAndBranchService;
import com.hp.octane.integrations.services.scmdata.SCMDataService;
import com.hp.octane.integrations.services.sonar.SonarService;
import com.hp.octane.integrations.services.entities.EntitiesService;
import com.hp.octane.integrations.services.events.EventsService;
import com.hp.octane.integrations.services.logs.LogsService;
import com.hp.octane.integrations.services.pipelines.PipelineContextService;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.services.tasking.TasksProcessor;
import com.hp.octane.integrations.services.tests.TestsService;
import com.hp.octane.integrations.services.vulnerabilities.VulnerabilitiesService;

/**
 * OctaneClient is a single entry point for an integration with specific Octane target (server AND shared space)
 * OctaneClient instance is responsible for a correct initialization/shutdown cycle and provisioning of a services in the concrete context
 * OctaneClient instance's context is defined by a specific instance of CIPluginServices
 */

public interface OctaneClient extends HasMetrics {

	/**
	 * provides OctaneClient instance ID
	 * @return instance ID
	 */
	String getInstanceId();

	/**
	 * check whether SDK version is supported by Octane
	 */
	void refreshSdkSupported();

	/**
	 * provides Configuration service
	 *
	 * @return service, MUST NOT be null
	 */
	ConfigurationService getConfigurationService();

	/**
	 * provides Coverage service
	 *
	 * @return service, MUST NOT be null
	 */
	CoverageService getCoverageService();

	/**
	 * provides Sonar integration service
	 *
	 * @return service, MUST NOT be null
	 */
	SonarService getSonarService();

	/**
	 * provides Entities service
	 *
	 * @return service, MUST NOT be null
	 */
	EntitiesService getEntitiesService();

	/**
	 * provides Bridge service (task polling)
	 *
	 * @return service, MUST NOT be null
	 */
	BridgeService getBridgeService();

	/**
	 * provides PipelineContextImpl service
	 *
	 * @return service, MUST NOT be null
	 */
	PipelineContextService getPipelineContextService();

	/**
	 * provides Events service
	 *
	 * @return service, MUST NOT be null
	 */
	EventsService getEventsService();

	/**
	 * provides Logs service
	 *
	 * @return service, MUST NOT be null
	 */
	LogsService getLogsService();

	/**
	 * provides REST service
	 *
	 * @return service, MUST NOT be null
	 */
	RestService getRestService();

	/**
	 * provides Tasks service
	 *
	 * @return service, MUST NOT be null
	 */
	TasksProcessor getTasksProcessor();

	/**
	 * provides Tests service
	 *
	 * @return service, MUST NOT be null
	 */
	TestsService getTestsService();


	/**
	 * provides PullRequest service
	 *
	 * @return service, MUST NOT be null
	 */
	PullRequestAndBranchService getPullRequestAndBranchService();

	/**
	 * provides Vulnerabilities service
	 *
	 * @return service, MUST NOT be null
	 */
	VulnerabilitiesService getVulnerabilitiesService();

	/**
	 *  provides SCMDataService service
	 *
	 * @return service, MUST NOT be null
	 */
	SCMDataService getSCMDataService();

	void validateOctaneIsActiveAndSupportVersion(String version);
}
