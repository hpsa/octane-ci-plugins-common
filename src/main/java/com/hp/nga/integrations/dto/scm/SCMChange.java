package com.hp.nga.integrations.dto.scm;

import com.hp.nga.integrations.dto.DTOBase;

/**
 * Created by gullery on 08/12/2015.
 * SCM Change descriptor
 */

public interface SCMChange extends DTOBase {

	String getType();

	SCMChange setType(String type);

	String getFile();

	SCMChange setFile(String file);
}
