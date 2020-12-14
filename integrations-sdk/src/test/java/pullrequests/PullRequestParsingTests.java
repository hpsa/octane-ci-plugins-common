package pullrequests;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PullRequestParsingTests {

    @Test
    public void testBitbucketServerParsing() throws IOException {
        String json = readResourceAsString("bitbucketServerPullRequests.json");
        com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.EntityCollection<com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.PullRequest> list = com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.JsonConverter.convertCollection(
                json,
                com.hp.octane.integrations.services.pullrequestsandbranches.bitbucketserver.pojo.PullRequest.class);
        Assert.assertEquals(11, list.getSize());

    }

    @Test
    public void testGithubServerParsing() throws IOException {
        String json = readResourceAsString("githubServerPullRequests.json");
        List<com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.PullRequest> list = com.hp.octane.integrations.services.pullrequestsandbranches.github.JsonConverter.convertCollection(json, com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.PullRequest.class);
        Assert.assertEquals(5, list.size());
    }

    @Test
    public void testGithubCloudParsing() throws IOException {
        String json = readResourceAsString("githubCloudPullRequests.json");
        List<com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.PullRequest> list = com.hp.octane.integrations.services.pullrequestsandbranches.github.JsonConverter.convertCollection(json, com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo.PullRequest.class);
        Assert.assertEquals(30, list.size());
    }

    public String readResourceAsString(String resourceName) throws IOException {
        InputStream inputStream = this.getClass().getResourceAsStream(resourceName);
        StringBuilder textBuilder = new StringBuilder();

        Reader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())));
        int c = 0;
        while ((c = reader.read()) != -1) {
            textBuilder.append((char) c);
        }
        return textBuilder.toString();
    }
}
