"""Send the events of an NDJSON file to Event Hubs through its Kafka endpoint (port 9093).

Every event goes to partition 0, in file order. The connection string comes from the environment. It is never printed.
`confluent-kafka` is imported only when you send for real. It is not needed for the tests.
Status: UNVERIFIED against a real Event Hubs until Azure Stage 2.

Run: python -m src.generator.send --events data/generated/A/events.ndjson --topic trade-events --start 0 --count 100
Needs the variables EVENTHUBS_NAMESPACE and EVENTHUBS_CONNECTION_STRING (use a send-only policy).
"""

import argparse
import os


def producer_config(namespace, connection_string):
    return {
        "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": connection_string,
    }


def send_events(lines, producer, topic, start=0, count=None):
    """Send lines[start:start+count]. Raise if any delivery fails. Return {"sent": n}."""
    chosen = [ln.strip() for ln in list(lines)[start : None if count is None else start + count] if ln.strip()]
    failed = []
    for line in chosen:
        producer.produce(topic, value=line.encode(), partition=0, on_delivery=lambda err, _msg: failed.append(err) if err else None)
        producer.poll(0)
    producer.flush()
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(chosen)} events were not delivered. First error: {failed[0]}")
    return {"sent": len(chosen)}


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--events", required=True)
    p.add_argument("--topic", default="trade-events")
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--count", type=int)
    a = p.parse_args()
    from confluent_kafka import Producer  # not needed for the tests

    producer = Producer(producer_config(os.environ["EVENTHUBS_NAMESPACE"], os.environ["EVENTHUBS_CONNECTION_STRING"]))
    with open(a.events, encoding="utf-8") as f:
        print(send_events(f.readlines(), producer, a.topic, a.start, a.count))


if __name__ == "__main__":
    main()
