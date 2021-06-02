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

package com.hp.octane.integrations.dto.scm.impl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hp.octane.integrations.dto.scm.SCMChange;

import java.util.LinkedList;
import java.util.List;

/**
 * SCM Change descriptor
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class SCMChangeImpl implements SCMChange {
	private String type;
	private String file;
	private List<LineRange> addedLines;
	private List<LineRange> deletedLines;
	private String renamedToFile;

	public String getType() {
		return type;
	}

	public SCMChange setType(String type) {
		this.type = type;
		return this;
	}

	public String getFile() {
		return file;
	}

	public List<LineRange> getAddedLines() {
		return addedLines;
	}

	public void setAddedLines(List<LineRange> addedLines) {
		this.addedLines = addedLines;
	}

	@Override
	public void insertAddedLines(LineRange newRange) {
		if (this.addedLines == null) {
			this.addedLines = new LinkedList<>();
		}
		this.addedLines.add(newRange);
	}

	@Override
	public void insertDeletedLines(LineRange newRange) {
		if (this.deletedLines == null) {
			this.deletedLines = new LinkedList<>();
		}
		this.deletedLines.add(newRange);
	}

	public List<LineRange> getDeletedLines() {
		return deletedLines;
	}

	public void setDeletedLines(List<LineRange> deletedLines) {
		this.deletedLines = deletedLines;
	}

	public SCMChange setFile(String file) {
		this.file = file;
		return this;
	}

	/**
	 * in case it's delete type (that came from renaming),
	 * we want to enrich the new renamed file as part of the SCMChange.
	 * this field will be filled as part of the lines enrichment process
	 *
	 * @param renamedToFile name of renamed file
	 */
	@Override
	public void setRenamedToFile(String renamedToFile) {
		this.renamedToFile = renamedToFile;
	}

	@Override
	public String getRenamedToFile() {
		return this.renamedToFile;
	}
}
