
import asyncio
import json
import uuid
from datetime import datetime, timezone

import requests
import websockets
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"

STORE_WS = "wss://api.sondahub.com/v1/store/ws?topics=orders"
SOCIAL_WS = "wss://api.sondahub.com/v1/social/ws?topics=likes"

API = "https://api.sondahub.com/v1/store"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=10
)

paid_orders = set()
item_cache = {}


def timestamp(value=None):
    return value or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def publish(topic, event):
    producer.send(
        topic,
        key=event["event_id"],
        value=event
    )
    print(f"[{topic}] {event}")


def mapped_customer(source_id):
    return 101 + ((int(source_id) - 1) % 5)


def mapped_product(source_id):
    return 201 + ((int(source_id) - 1) % 5)


def mapped_order(source_id):
    return 1_000_000 + int(source_id)


def mapped_payment(source_id):
    return 5_000_000 + int(source_id)


def get_order_item(order_id):
    if order_id not in item_cache:
        try:
            response = requests.get(
                f"{API}/order_items",
                params={"order_id": order_id, "limit": 1},
                timeout=10
            )
            response.raise_for_status()

            records = response.json().get("data", [])
            item_cache[order_id] = records[0] if records else {}

        except requests.RequestException as exc:
            print(f"Order item lookup failed: {exc}")
            item_cache[order_id] = {}

    return item_cache[order_id]


async def store_stream():
    while True:
        try:
            print("Connecting to SondaHub Store...")

            async with websockets.connect(
                STORE_WS,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30
            ) as ws:

                print("Store stream connected")

                async for raw in ws:
                    message = json.loads(raw)

                    if message.get("type") != "event":
                        continue

                    if message.get("topic") != "orders":
                        continue

                    order = message.get("data", {})

                    source_id = int(order["id"])
                    source_customer = int(order["customer_id"])

                    order_id = mapped_order(source_id)
                    customer_id = mapped_customer(source_customer)

                    item = await asyncio.to_thread(
                        get_order_item,
                        source_id
                    )

                    source_product = int(item.get("product_id", source_id))
                    product_id = mapped_product(source_product)

                    quantity = max(1, int(item.get("quantity", 1)))

                    amount = float(
                        order.get("total")
                        or order.get("subtotal")
                        or 0
                    )

                    status = str(order.get("status", "pending")).lower()

                    event = {
                        "event_id": str(uuid.uuid4()),
                        "event_type": "ORDER_UPDATED",
                        "event_timestamp": timestamp(message.get("ts")),
                        "order_id": order_id,
                        "customer_id": customer_id,
                        "product_id": product_id,
                        "quantity": quantity,
                        "amount": amount,
                        "status": status
                    }

                    publish("orders", event)

                    if status in ("paid", "shipped", "delivered"):
                        if source_id not in paid_orders:

                            payment = {
                                "event_id": str(uuid.uuid4()),
                                "event_type": "PAYMENT_CREATED",
                                "event_timestamp": timestamp(message.get("ts")),
                                "payment_id": mapped_payment(source_id),
                                "order_id": order_id,
                                "amount": amount,
                                "payment_method": order.get(
                                    "payment_method", "card"
                                ),
                                "status": "SUCCESS"
                            }

                            publish("payments", payment)
                            paid_orders.add(source_id)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(f"Store stream error: {exc}")
            await asyncio.sleep(5)


async def social_stream():
    while True:
        try:
            print("Connecting to SondaHub Social...")

            async with websockets.connect(
                SOCIAL_WS,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30
            ) as ws:

                print("Social stream connected")

                async for raw in ws:
                    message = json.loads(raw)

                    if message.get("type") != "event":
                        continue

                    if message.get("topic") != "likes":
                        continue

                    data = message.get("data", {})

                    customer_id = mapped_customer(data["user_id"])

                    event = {
                        "event_id": str(uuid.uuid4()),
                        "event_type": "PAGE_VIEW",
                        "event_timestamp": timestamp(message.get("ts")),
                        "customer_id": customer_id,
                        "session_id": f"social-{data['user_id']}",
                        "page": f"/posts/{data['post_id']}",
                        "device": "web"
                    }

                    publish("clickstream", event)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(f"Social stream error: {exc}")
            await asyncio.sleep(5)


async def main():
    try:
        await asyncio.gather(
            store_stream(),
            social_stream()
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Producer stopped")
