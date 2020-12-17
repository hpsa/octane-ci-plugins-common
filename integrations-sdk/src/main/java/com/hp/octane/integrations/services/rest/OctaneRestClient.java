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

package com.hp.octane.integrations.services.rest;

import com.hp.octane.integrations.OctaneConfiguration;
import com.hp.octane.integrations.dto.connectivity.OctaneRequest;
import com.hp.octane.integrations.dto.connectivity.OctaneResponse;
import com.hp.octane.integrations.services.HasMetrics;

import java.io.IOException;

public interface OctaneRestClient extends HasMetrics {

	String CLIENT_TYPE_HEADER = "HPECLIENTTYPE";
	String CLIENT_TYPE_VALUE = "HPE_CI_CLIENT";

	/**
	 * Executes Octane server oriented request based on the pre-configuration
	 *
	 * @param request request
	 * @return OctaneResponse
	 * @throws IOException exception during connectivity or (de)serialization
	 */
	OctaneResponse execute(OctaneRequest request) throws IOException;

	/**
	 * Executes Octane server oriented request based on the provided configuration
	 *
	 * @param request request
	 * @param configuration configuration
	 * @return OctaneResponse
	 * @throws IOException exception during connectivity or (de)serialization
	 */
	OctaneResponse execute(OctaneRequest request, OctaneConfiguration configuration) throws IOException;

	/**
	 * Shuts down the REST client
	 */
	void shutdown();
}
