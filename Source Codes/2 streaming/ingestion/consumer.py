
import json
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata

BOOTSTRAP_SERVERS = "localhost:9092"
BUCKET = "ecommerce-lakehouse-1482"

TOPICS = ["orders", "payments", "clickstream"]

s3 = boto3.client("s3")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="ecommerce-s3-consumer",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    max_poll_records=100
)

print("Kafka-to-S3 consumer started")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)

        for tp, messages in records.items():
            if not messages:
                continue

            for message in messages:
                event = message.value

                event["_kafka_topic"] = message.topic
                event["_kafka_partition"] = message.partition
                event["_kafka_offset"] = message.offset
                event["_ingested_at"] = datetime.now(
                    timezone.utc
                ).isoformat()

                key = (
                    f"raw/streaming/{message.topic}/"
                    f"partition={message.partition}/"
                    f"offset={message.offset}.json"
                )

                s3.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=json.dumps(event).encode("utf-8"),
                    ContentType="application/json"
                )

                print(
                    f"Uploaded {message.topic} "
                    f"partition={message.partition} "
                    f"offset={message.offset}"
                )

            # Commit only after every event in this partition was uploaded.
            last_offset = messages[-1].offset

            consumer.commit({
                tp: OffsetAndMetadata(last_offset + 1, "")
            })

            print(f"Committed {tp.topic}: offset {last_offset + 1}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    print("Consumer closed")
