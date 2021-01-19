package com.hp.octane.integrations.exceptions;

import com.hp.octane.integrations.dto.entities.OctaneRestExceptionData;
import com.hp.octane.integrations.utils.SdkStringUtils;

public class OctaneRestException extends RuntimeException {
    private OctaneRestExceptionData data;
    private int responseStatus;

    public OctaneRestException(int responseStatus, OctaneRestExceptionData data) {
        super(SdkStringUtils.isNotEmpty(data.getDescriptionTranslated()) ? data.getDescriptionTranslated() : data.getDescription());
        this.data = data;
        this.responseStatus = responseStatus;
    }

    public OctaneRestExceptionData getData() {
        return data;
    }

    public int getResponseStatus() {
        return responseStatus;
    }
}
