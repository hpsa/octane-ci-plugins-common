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

import com.hp.octane.integrations.services.configuration.ConfigurationService;
import com.hp.octane.integrations.services.entities.EntitiesService;
import com.hp.octane.integrations.services.events.EventsService;
import com.hp.octane.integrations.services.logs.LogsService;
import com.hp.octane.integrations.services.rest.RestService;
import com.hp.octane.integrations.services.tasking.TasksProcessor;
import com.hp.octane.integrations.services.tests.TestsService;
import com.hp.octane.integrations.services.vulnerabilities.VulnerabilitiesService;

/**
 * OctaneClient is a single entry point for an integration with specific Octane target (server AND shared space)
 * OctaneClient instance is responsible for a correct initialization/shutdown cycle and provisioning of a services in the concrete context
 * OctaneClient instance's context is defined by a specific instance of CIPluginServices
 */

public interface OctaneClient {

	/**
	 * provides Configuration service
	 *
	 * @return service, MUST NOT be null
	 */
	ConfigurationService getConfigurationService();

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
	 * provides Events service
	 *
	 * @return service, MUST NOT be null
	 */
	EventsService getEventsService();

	/**
	 * provides Tests service
	 *
	 * @return service, MUST NOT be null
	 */
	TestsService getTestsService();

	/**
	 * provides Logs service
	 *
	 * @return service, MUST NOT be null
	 */
	LogsService getLogsService();

	/**
	 * provides Vulnerabilities service
	 *
	 * @return service, MUST NOT be null
	 */
	VulnerabilitiesService getVulnerabilitiesService();

	/**
	 * provides Entities service
	 *
	 * @return service, MUST NOT be null
	 */
	EntitiesService getEntitiesService();

	/**
	 * resolves instance ID from the plugin services
	 * pay attention: this property is mutable on the plugin's side, therefore we'll be resolving it at runtime
	 *
	 * @return instance ID; won't be NULL, if the underlying plugin service will resolve instance ID to NULL, this method should throw IllegalStateException
	 */
	String getEffectiveInstanceId() throws IllegalStateException;

	/**
	 * closes this OctaneClient instance, while shutting down all of its services/tasks/rest calls
	 */
	void close();
}