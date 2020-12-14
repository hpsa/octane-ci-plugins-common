package com.hp.octane.integrations.services.pullrequestsandbranches.factory;

public interface CommitUserIdPicker {

    String getUserIdForCommit(String email, String name);
}
