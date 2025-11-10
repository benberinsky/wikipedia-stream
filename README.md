# Streaming Data

## Wikipedia Events API

![Wikipedia Event Stream](https://diff.wikimedia.org/wp-content/uploads/2017/03/eventstreamseditcharts.gif)

The Wikipedia Events data stream traces events that occur within the MediaWiki ecosystem. The stream passes approximately 30-50 events/sec.

Use this repository to:

- Run a custom Kafka cluster, along with a Redpanda console and a Redis database instance.
- Create a Kafka producer that listens to the SSE stream and passes them as entries into a Kafka topic.
- Create a Kafka consumer that reads out messages from the topic and identifies the event "type" for each.
- Store a running count of each event type in a Redis key-value store.

**What is SSE?** Server-Sent Events (SSE) is a standardized, lightweight, one-way communication protocol that allows a server to push real-time data updates to a web client over a persistent HTTP connection using WebSockets.

- [Learn more](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service) about the Wikipedia EventStream.

## Getting Started

1. [**Fork**](https://github.com/uvasds-systems/wikipedia-stream/fork) and clone this repository to your local machine.
2. Create a Python virtual environment, activate it, and install all packages in `requirements.txt`.
3. Using Docker, run this command from within the directory of your clone:

    ```
    docker compose up
    ```

4. This will create a new Docker network and start 5 containers running on that network, along with Docker storage volumes for each Kafka node.
5. The Kafka UI is available at http://127.0.0.1:8080/
6. Once up and running, start listening to the event stream and producing messages into Kafka:

    ```
    python wikipedia-stream.py
    ```

7. Visit the Kafka UI and check the list of topics. Click into the `wikipedia-edits` topic and watch messages as they are written.
8. Next, use the consumer to read out messages, extract the `type` data field from the message JSON, and store it in Redis.

    ```
    python wikipedia-consumer.py
    ```

9. Your consumer will process messages as quickly as it is able. Given enough time it will catch up with the total number of messages published by the producer.
