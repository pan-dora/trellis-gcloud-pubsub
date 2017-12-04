# trellis-gcloud-pubsub

An implementation of Trellis EventService that publishes events to Google Cloud PubSub.

## Configuration

Set property `GOOGLE_APPLICATION_CREDENTIALS` in `gradle.properties` to the path of the PubSub service
admin credential file.

See https://developers.google.com/identity/protocols/application-default-credentials for details.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install

## Testing
The IT depends on Annotation Processing and Google Auto.  If your IDE does not generate the symbol, check this
issue for information: https://github.com/google/auto/issues/106  
