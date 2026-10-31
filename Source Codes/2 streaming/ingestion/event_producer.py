import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

def publish(topic, event):
    producer.send(
        topic,
        key=event["event_id"],
        value=event
    )
    print(f"Published: {topic} | {event['event_id']}")

def base_event(event_type):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

try:
    # Orders
    for i in range(1, 6):
        event = base_event("ORDER_CREATED")
        event.update({
            "order_id": 2000 + i,
            "customer_id": 101 + (i % 5),
            "product_id": 201 + (i % 5),
            "quantity": i,
            "amount": 2500.0 * i,
            "status": "CREATED"
        })
        publish("orders", event)

    # Payments
    for i in range(1, 6):
        event = base_event("PAYMENT_CREATED")
        event.update({
            "payment_id": 6000 + i,
            "order_id": 2000 + i,
            "amount": 2500.0 * i,
            "payment_method": "UPI",
            "status": "SUCCESS"
        })
        publish("payments", event)

    # Clickstream
    for i in range(1, 6):
        event = base_event("PAGE_VIEW")
        event.update({
            "customer_id": 101 + (i % 5),
            "session_id": f"session_{i}",
            "page": "/products/laptop",
            "device": "desktop"
        })
        publish("clickstream", event)

    producer.flush()
    print("\nAll events published successfully.")

finally:
    producer.close()