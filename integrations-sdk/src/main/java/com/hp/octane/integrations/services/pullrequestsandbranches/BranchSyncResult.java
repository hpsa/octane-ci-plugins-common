package com.hp.octane.integrations.services.pullrequestsandbranches;

import com.hp.octane.integrations.dto.scm.Branch;

import java.util.ArrayList;
import java.util.List;

public class BranchSyncResult {

    private List<Branch> deleted = new ArrayList<>();

    private List<Branch> created = new ArrayList<>();

    private List<Branch> updated = new ArrayList<>();

    private boolean hasSkipped;

    private String firstSkipped;

    public List<Branch> getDeleted() {
        return deleted;
    }

    public List<Branch> getCreated() {
        return created;
    }

    public List<Branch> getUpdated() {
        return updated;
    }

    public boolean getHasSkipped() {
        return hasSkipped;
    }

    public void setHasSkipped(boolean hasSkipped) {
        this.hasSkipped = hasSkipped;
    }

    public String getFirstSkipped() {
        return firstSkipped;
    }

    public void setFirstSkipped(String firstSkipped) {
        this.firstSkipped = firstSkipped;
    }
}
