from quixstreams import Application
import os
import json
from requests_sse import EventSource
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WIKIPEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


new_event_producer = Application(
            broker_address=KAFKA_BROKER,
            producer_group = "new_event_producer",
            consumer_group="wikipedia-producer",
            producer_extra_config={
                # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
                "broker.address.family": "v4",
            }
        )

topic = new_event_producer.topic(
            name="new_events",
            value_serializer="json",
        )
print(f"Connected to Kafka broker at {KAFKA_BROKER}")
print(f"Producing to topic: {topic.name}")


def publish_to_new_topic(change_event: dict) -> bool:
    """Publish event to 'new' topic."""
    try:
        event_id = str(change_event.get('id', ''))
        serialized = new_event_producer.serialize(key=event_id, value=change_event)
        with new_event_producer.get_producer() as producer:
            producer.produce(
                topic=new_event_producer.name,
                key=serialized.key,
                value=serialized.value
            )
        return True
    except Exception as e:
        print(f"Error publishing to 'new' topic: {e}")
        return False


def publish_to_new_topic(change_event: dict) -> bool:
    """Publish event to 'categorize' topic."""
    try:
        event_id = str(change_event.get('id', ''))
        serialized = topic.serialize(key=event_id, value=change_event)
        with new_event_producer.get_producer() as producer:
            producer.produce(
                topic=topic.name,
                key=serialized.key,
                value=serialized.value
            )
        return True
    except Exception as e:
        print(f"Error publishing to 'categorize' topic: {e}")
        return False

def process_change(change_event: dict) -> None:
    """
    Process a single Wikipedia change event and route to appropriate topic.
    
    Args:
        change_event: Dictionary containing the change event data
    """
    # Extract key information
    event_type = change_event.get('type', 'unknown')
    title = change_event.get('title', 'N/A')
    user = change_event.get('user', 'anonymous')
    wiki = change_event.get('server_name', 'unknown')
    timestamp = change_event.get('timestamp')
    
    # Print the change
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{event_type}] {user} edited '{title}' on {wiki}")
    
    # Route to appropriate topic based on event type
    published = False
    if event_type == 'new':
        published = publish_to_new_topic(change_event)
        if published:
            print(f"  ✓ Published to 'new' topic (ID: {change_event.get('id')})")
    
    if not published:
        print(f"  ✗ Failed to publish event (ID: {change_event.get('id')})")


def run():
    """
    Connect to Wikipedia SSE stream and continuously process events.
    """
    print(f"Connecting to Wikipedia SSE stream: {WIKIPEDIA_SSE_URL}")
    print("Press Ctrl+C to stop\n")
    print("Topics available: new, categorize, edits, other\n")
    
    try:
        # Set up headers with proper User-Agent (required by Wikimedia)
        headers = {
            'User-Agent': 'WikipediaStreamer/1.0 (Educational Project; Python/requests-sse)',
            'Accept': 'text/event-stream'
        }
        
        # Connect to the SSE stream with headers
        with EventSource(WIKIPEDIA_SSE_URL, headers=headers) as source:
            for event in source:
                # The message data is in event.data as a JSON string
                if event.data:
                    try:
                        change_event = json.loads(event.data)
                        process_change(change_event)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing event: {e}")
                    except Exception as e:
                        print(f"Error processing event: {e}")
                        
    except KeyboardInterrupt:
        print("\n\nStopping stream...")
    except Exception as e:
        print(f"Error connecting to stream: {e}")
        raise


if __name__ == "__main__":
    run()
