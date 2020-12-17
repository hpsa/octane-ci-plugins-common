package com.hp.octane.integrations.dto.entities;

public class EntityConstants {

    public static class Base {
        public static final String ID_FIELD = "id";
        public static final String NAME_FIELD = "name";
        public static final String LOGICAL_NAME_FIELD = "logical_name";
        public static final String DESCRIPTION_FIELD = "description";
        public static final String TYPE_FIELD_NAME = "type";
    }

    public static class AutomatedTest extends Base {
        public static final String COLLECTION_NAME = "automated_tests";
        public static final String ENTITY_NAME = "automated_test";

        public static final String TEST_RUNNER_FIELD = "test_runner";
        public static final String SCM_REPOSITORY_FIELD = "scm_repository";
        public static final String TESTING_TOOL_TYPE_FIELD = "testing_tool_type";
        public static final String TEST_TYPE_FIELD = "test_type";
        public static final String FRAMEWORK_FIELD = "framework";
        public static final String PACKAGE_FIELD = "package";
        public static final String EXECUTABLE_FIELD = "executable";
    }

    public static class ScmResourceFile extends Base {
        public static final String COLLECTION_NAME = "scm_resource_files";
        public static final String ENTITY_NAME = "scm_resource_file";

        public static final String RELATIVE_PATH_FIELD = "relative_path";
        public static final String SCM_REPOSITORY_FIELD = "scm_repository";
    }

    public static class ScmRepository extends Base {
        public static final String COLLECTION_NAME = "scm_repositories";
        public static final String ENTITY_NAME = "scm_repository";
        public static final String BRANCH_FIELD = "branch";
        public static final String PARENT_FIELD = "repository";

        public static final String IS_MERGED_FIELD = "is_merged";
        public static final String IS_DELETED_FIELD = "is_deleted";
        public static final String LAST_COMMIT_SHA_FIELD = "last_commit_sha";
        public static final String LAST_COMMIT_TIME_FIELD = "last_commit_time";
        public static final String SCM_USER_FIELD = "user";

    }

    public static class ScmRepositoryRoot extends Base {
        public static final String COLLECTION_NAME = "scm_repository_roots";
        public static final String ENTITY_NAME = "scm_repository_root";
        public static final String URL_FIELD = "url";
    }

    public static class Executors extends Base {
        public static final String COLLECTION_NAME = "executors";
		public static final String ENTITY_NAME = "executor";
        public static final String TEST_RUNNER_SUBTYPE_ENTITY_NAME = "test_runner";
        public static final String UFT_TEST_RUNNER_SUBTYPE_ENTITY_NAME = "uft_test_runner";
    }

    public static class Taxonomy extends Base {
        public static final String COLLECTION_NAME = "taxonomy_nodes";
        public static final String CATEGORY_NAME = "category";
    }

    public static class Release extends Base {
        public static final String COLLECTION_NAME = "releases";
    }

    public static class Milestone extends Base {
        public static final String COLLECTION_NAME = "milestones";
        public static final String RELEASE_FIELD = "release";
    }

    public static class Lists extends Base {
        public static final String COLLECTION_NAME = "list_nodes";
    }

    public static class Workspaces extends Base {
        public static final String COLLECTION_NAME = "workspaces";
    }
}
