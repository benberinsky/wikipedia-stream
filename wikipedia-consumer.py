from quixstreams import Application
import json
global edit


def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="wikipedia-changes",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-changes"])
        edit = 0
        categorize = 0
        log = 0
        delete = 0
        new = 0
        other = 0
        
        while True:
            try:
                msg = consumer.poll(1)

                if msg is None:
                    print("Waiting...")
                elif msg.error() is not None:
                    raise Exception(msg.error())
                else:
                    key = msg.key().decode("utf8")
                    value = json.loads(msg.value())
                    offset = msg.offset()
                    event_type = value.get('type', 'unknown')

                    print(f"{key} {event_type}")
                    #consumer.store_offsets(event_type)
                    if event_type == 'edit':
                        edit += 1
                    elif event_type == 'categorize':
                        categorize+=1
                    elif event_type == 'log':
                        log +=1
                    else:
                        other +=1
            except KeyboardInterrupt:
                print(f"Edits: {edit}, Categorizes: {categorize}, Logs: {log}, Others: {other} ")
                pass
                break
                


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
