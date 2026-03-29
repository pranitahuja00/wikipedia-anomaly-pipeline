import json
import os
import time
import requests
from dotenv import load_dotenv
from sseclient import SSEClient
from confluent_kafka import Producer

load_dotenv()

# --- Confluent Cloud config ---
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = { "User-Agent": "wikipedia-anomaly-pipeline" }

PAGE_EVENT_TYPES = {"categorize", "log"}
EDIT_EVENT_TYPES = {"edit", "new"}

producer = Producer(KAFKA_CONFIG)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")


def parse_and_route(event_data: dict) -> tuple[str, dict] | None:
    """
    Decides which topic an event belongs to and strips
    fields we do not need, keeping the payload lean.
    """
    event_type = event_data.get("type")

    if event_type in EDIT_EVENT_TYPES:
        payload = {
            "id":           event_data.get("id"),
            "title":        event_data.get("title"),
            "wiki":         event_data.get("wiki"),
            "namespace":    event_data.get("namespace"),
            "user":         event_data.get("user"),
            "bot":          event_data.get("bot"),
            "minor":        event_data.get("minor"),
            "timestamp":    event_data.get("timestamp"),
            "comment":      event_data.get("comment", ""),
            "length_old":   (event_data.get("length") or {}).get("old"),
            "length_new":   (event_data.get("length") or {}).get("new"),
            "revision_old": (event_data.get("revision") or {}).get("old"),
            "revision_new": (event_data.get("revision") or {}).get("new"),
            "server_name":  event_data.get("server_name"),
        }
        return "wiki_edits", payload

    elif event_type in PAGE_EVENT_TYPES:
        payload = {
            "id":           event_data.get("id"),
            "title":        event_data.get("title"),
            "wiki":         event_data.get("wiki"),
            "namespace":    event_data.get("namespace"),
            "user":         event_data.get("user"),
            "bot":          event_data.get("bot"),
            "timestamp":    event_data.get("timestamp"),
            "comment":      event_data.get("comment", ""),
            "log_type":     event_data.get("log_type"),
            "log_action":   event_data.get("log_action"),
            "server_name":  event_data.get("server_name"),
        }
        return "wiki_page_events", payload

    return None


def run():
    print("Starting Wikipedia Kafka producer...")
    messages_sent = 0

    while True:
        try:
            response = requests.get(WIKI_STREAM_URL, stream=True, headers=HEADERS)
            client = SSEClient(response)

            for event in client.events():
                if not event.data or event.data.strip() == "":
                    continue

                try:
                    data = json.loads(event.data)
                except json.JSONDecodeError:
                    continue

                # Filter to English Wikipedia only to keep volume manageable
                if data.get("server_name") != "en.wikipedia.org":
                    continue

                result = parse_and_route(data)
                if result is None:
                    continue

                topic, payload = result

                producer.produce(
                    topic=topic,
                    key=str(payload.get("id", "")),
                    value=json.dumps(payload),
                    callback=delivery_report,
                )

                # Poll to trigger delivery callbacks without blocking
                producer.poll(0)
                messages_sent += 1

                if messages_sent % 100 == 0:
                    producer.flush()

        except Exception as e:
            print(f"Stream error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    try:
        run()
    finally:
        print("Shutting down, flushing remaining messages...")
        producer.flush()
        print("Done.")
