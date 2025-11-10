from quixstreams import Application
from datetime import timedelta, datetime

# Create the Quix Application (connects to Kafka)
app = Application(
    broker_address='127.0.0.1:19092',
    consumer_group='wikipedia-tumbling-window-v1',
    auto_offset_reset='earliest',
)

# Define the input topic
wikipedia_topic = app.topic('wikipedia-edits', value_deserializer='json')

# Create a streaming dataframe
sdf = app.dataframe(wikipedia_topic)

# Filter out events without a valid 'type' field
sdf = sdf.filter(lambda event: event.get('type') is not None and event.get('type') != '')

# Group by event type and create a 1-minute tumbling window to count events
# Using reduce to preserve the event type in the result
def initializer(event):
    return {'type': event.get('type', 'unknown'), 'count': 0}

def reducer(aggregated, event):
    aggregated['count'] += 1
    return aggregated

sdf = (
    sdf.group_by(lambda event: event['type'], name='type')
    .tumbling_window(duration_ms=timedelta(minutes=1))
    .reduce(initializer=initializer, reducer=reducer)
    .final()
)

# Print results when windows close
def print_window_result(result):
    """Print the aggregated results when a window closes."""
    start_time = datetime.fromtimestamp(result['start'] / 1000).strftime('%H:%M:%S')
    end_time = datetime.fromtimestamp(result['end'] / 1000).strftime('%H:%M:%S')
    
    print(f"{'='*50}")
    print(f"Window:     {start_time} to {end_time} EST")
    print(f"Event Type: {result['value']['type']}")
    print(f"Count:      {result['value']['count']}")
    print(f"{'='*50}\n")

sdf.update(print_window_result)

if __name__ == '__main__':
    print("Starting Wikipedia event type counter with 1-minute tumbling windows...")
    print("Press Ctrl+C to stop\n")
    app.run(sdf)
