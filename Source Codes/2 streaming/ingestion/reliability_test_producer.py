import json
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
    retries=5
)

now = datetime.now(timezone.utc)
suffix = str(int(time.time()))

def timestamp(dt):
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

current_ts = timestamp(now)
late_ts = timestamp(now - timedelta(days=10))
base_id = int(suffix[-6:])

events = {
    "orders": [
        {
            "event_id": f"RELTEST-ORDER-DUP-{suffix}",
            "order_id": 990000000 + base_id,
            "customer_id": 101,
            "product_id": 201,
            "quantity": 1,
            "amount": 125.50,
            "event_timestamp": current_ts
        },
        {
            "event_id": f"RELTEST-ORDER-LATE-{suffix}",
            "order_id": 991000000 + base_id,
            "customer_id": 101,
            "product_id": 201,
            "quantity": 1,
            "amount": 75.00,
            "event_timestamp": late_ts
        },
        {
            "event_id": f"RELTEST-ORDER-RECOVERY-{suffix}",
            "order_id": 992000000 + base_id,
            "customer_id": 101,
            "product_id": 201,
            "quantity": 2,
            "amount": 250.00,
            "event_timestamp": current_ts
        }
    ],
    "payments": [
        {
            "event_id": f"RELTEST-PAY-DUP-{suffix}",
            "payment_id": 993000000 + base_id,
            "order_id": 990000000 + base_id,
            "amount": 125.50,
            "status": "SUCCESS",
            "event_timestamp": current_ts
        },
        {
            "event_id": f"RELTEST-PAY-LATE-{suffix}",
            "payment_id": 994000000 + base_id,
            "order_id": 991000000 + base_id,
            "amount": 75.00,
            "status": "SUCCESS",
            "event_timestamp": late_ts
        },
        {
            "event_id": f"RELTEST-PAY-RECOVERY-{suffix}",
            "payment_id": 995000000 + base_id,
            "order_id": 992000000 + base_id,
            "amount": 250.00,
            "status": "SUCCESS",
            "event_timestamp": current_ts
        }
    ],
    "clickstream": [
        {
            "event_id": f"RELTEST-CLICK-DUP-{suffix}",
            "customer_id": 101,
            "session_id": f"RELTEST-SESSION-DUP-{suffix}",
            "page": "/reliability-test",
            "event_timestamp": current_ts
        },
        {
            "event_id": f"RELTEST-CLICK-LATE-{suffix}",
            "customer_id": 101,
            "session_id": f"RELTEST-SESSION-LATE-{suffix}",
            "page": "/late-event-test",
            "event_timestamp": late_ts
        },
        {
            "event_id": f"RELTEST-CLICK-RECOVERY-{suffix}",
            "customer_id": 101,
            "session_id": f"RELTEST-SESSION-RECOVERY-{suffix}",
            "page": "/recovery-test",
            "event_timestamp": current_ts
        }
    ]
}

print("Publishing 12 reliability messages...")

for topic, topic_events in events.items():
    duplicate = topic_events[0]

    producer.send(topic, value=duplicate)
    producer.send(topic, value=duplicate)

    producer.send(topic, value=topic_events[1])
    producer.send(topic, value=topic_events[2])

producer.flush()
producer.close()

print("\nPUBLISHED EVENT IDS")
for topic, topic_events in events.items():
    print(f"\n{topic.upper()}")
    for event in topic_events:
        print(event["event_id"])

print("\nExpected: 4 Kafka messages per topic, 12 total.")