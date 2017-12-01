package org.trellisldp.gcloud.pubsub;

import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.trellisldp.api.ActivityStreamService;
import org.trellisldp.api.Event;
import org.trellisldp.api.EventService;

/**
 * A Google Cloud PubSub message producer capable of publishing messages to a Google Cloud PubSub.
 *
 * @author christopher-johnson
 */
public class GCloudPublisher implements EventService {

    private static final Logger LOGGER = getLogger(GCloudPublisher.class);

    // TODO - JDK9 ServiceLoader::findFirst
    private static ActivityStreamService service = ServiceLoader.load(
            ActivityStreamService.class).iterator().next();

    private final Publisher publisher;
    private final String topicName;

    /**
     * Create a new GCloud Publisher
     *
     * @param publisher the producer
     * @param topicName the name of the topic
     */
    public GCloudPublisher(final Publisher publisher, final String topicName) {
        requireNonNull(publisher);
        requireNonNull(topicName);

        this.publisher = publisher;
        this.topicName = topicName;
    }

    @Override
    public void emit(final Event event) {
        requireNonNull(event, "Cannot emit a null event!");

        service.serialize(event).ifPresent(message -> {
            LOGGER.debug("Sending message to Google Cloud PubSub topic: {}", topicName);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(
                        ByteString.copyFromUtf8(message)).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        });
    }
}

