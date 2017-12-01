/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trellisldp.gcloud.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static java.util.Collections.singleton;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import org.trellisldp.api.Event;
import org.trellisldp.api.EventService;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

import java.util.List;


/**
 * @author christopher-johnson
 */
@RunWith(JUnitPlatform.class)
public class GCloudPublisherTest {
    private static final String NAME_SUFFIX = UUID.randomUUID().toString();
    private static final RDF rdf = new SimpleRDF();
    private static String projectId;
    private final String topic = formatForTest("topic-test");
    private final String subscription = formatForTest("subscription-test");
    private static TopicAdminClient topicAdminClient;
    private static SubscriptionAdminClient subscriptionAdminClient;

    @Mock
    private Event mockEvent;

    @BeforeEach
    public void setUp() {
        initMocks(this);
        when(mockEvent.getTarget()).thenReturn(of(rdf.createIRI("trellis:repository/resource")));
        when(mockEvent.getAgents()).thenReturn(singleton(Trellis.RepositoryAdministrator));
        when(mockEvent.getIdentifier()).thenReturn(rdf.createIRI("urn:test"));
        when(mockEvent.getTypes()).thenReturn(singleton(AS.Update));
        when(mockEvent.getTargetTypes()).thenReturn(singleton(LDP.RDFSource));
        when(mockEvent.getInbox()).thenReturn(empty());
        projectId = ServiceOptions.getDefaultProjectId();
        try {
            topicAdminClient = buildTopicClient();
            subscriptionAdminClient = buildSubClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String formatForTest(String resourceName) {
        return resourceName + "-" + NAME_SUFFIX;
    }

    private Credentials getCredentials() throws IOException {
        Credentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(
                "/home/christopher/IdeaProjects/trellis-deployment/trellis-gcloud/app-credentials"
                        + "/trellisldp-b4f1e6de80c1.json"));
        return credentials;
    }

    private TopicAdminClient buildTopicClient() throws IOException {
        TopicAdminSettings topicAdminSettings =
                TopicAdminSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials()))
                        .build();
        return TopicAdminClient.create(topicAdminSettings);
    }

    private SubscriptionAdminClient buildSubClient() throws IOException {
        SubscriptionAdminSettings subscriptionAdminSettings =
                SubscriptionAdminSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials()))
                        .build();
        return SubscriptionAdminClient.create(subscriptionAdminSettings);
    }

    @Test
    public void testGCloudPublisher() throws IOException {
        TopicName topicName =
                TopicName.of(projectId, topic);
        SubscriptionName subscriptionName =
                SubscriptionName.of(projectId, subscription);
        topicAdminClient.createTopic(topicName);
        Publisher publisher = Publisher.newBuilder(topicName).build();
        final EventService svc = new GCloudPublisher(publisher, topic);
        svc.emit(mockEvent);
        subscriptionAdminClient.createSubscription(
                subscriptionName, topicName, PushConfig.newBuilder().build(), 10);

        final BlockingQueue<Object> receiveQueue = new LinkedBlockingQueue<>();
        Subscriber subscriber =
                Subscriber.newBuilder(
                        subscriptionName,
                        new MessageReceiver() {
                            @Override
                            public void receiveMessage(
                                    final PubsubMessage message, final AckReplyConsumer consumer) {
                                receiveQueue.offer(MessageAndConsumer.create(message, consumer));
                            }
                        })
                        .build();
        subscriber.addListener(
                new Subscriber.Listener() {
                    public void failed(Subscriber.State from, Throwable failure) {
                        receiveQueue.offer(failure);
                    }
                },
                MoreExecutors.directExecutor());
    }
}

