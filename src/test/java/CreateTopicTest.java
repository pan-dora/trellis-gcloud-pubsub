import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.TopicName;
import java.io.FileInputStream;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateTopicTest {
    private static final String NAME_SUFFIX = UUID.randomUUID().toString();
    private static TopicAdminClient topicAdminClient;
    private static SubscriptionAdminClient subscriptionAdminClient;
    private static String projectId;

    @BeforeClass
    public static void setupClass() throws Exception {
        topicAdminClient = TopicAdminClient.create();
        subscriptionAdminClient = SubscriptionAdminClient.create();
        projectId = ServiceOptions.getDefaultProjectId();
    }

    private String formatForTest(String resourceName) {
        return resourceName + "-" + NAME_SUFFIX;
    }

    @Test
    public void createTopicTest() throws Exception {
        Credentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(
                "/home/christopher/IdeaProjects/trellis-deployment/trellis-gcloud/app-credentials"
                        + "/trellisldp-b4f1e6de80c1.json"));
        TopicAdminSettings topicAdminSettings =
                TopicAdminSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .build();
        TopicName topic = TopicName.of(projectId, formatForTest("test-topic"));
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            topicAdminClient.createTopic(topic);
        }
    }
}
