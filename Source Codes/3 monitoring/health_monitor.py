
import os
import json
import time
import smtplib
import logging
import subprocess
import urllib.request
import urllib.error

from pathlib import Path
from datetime import datetime, timezone
from email.message import EmailMessage

import boto3
import psutil
from kafka import KafkaAdminClient


# ---------------- CONFIGURATION ----------------

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
STATE_FILE = LOG_DIR / "monitor_state.json"

LOG_DIR.mkdir(exist_ok=True)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

BUCKET = "ecommerce-lakehouse-1482"
TOPICS = ["orders", "payments", "clickstream"]

S3_MAX_AGE_MINUTES = int(os.getenv("S3_MAX_AGE_MINUTES", "60"))
JOB_MAX_AGE_MINUTES = int(os.getenv("JOB_MAX_AGE_MINUTES", "60"))

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "")

SMTP_SERVER = os.getenv("SMTP_SERVER", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_FROM = os.getenv("ALERT_FROM", SMTP_USER)
ALERT_TO = os.getenv("ALERT_TO", "")

logging.basicConfig(
    filename=LOG_DIR / "health_monitor.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ---------------- ALERT MANAGEMENT ----------------

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def send_email(subject, body):
    if not all([SMTP_SERVER, SMTP_USER, SMTP_PASSWORD, ALERT_TO]):
        logging.warning("Email configuration missing. Alert not emailed.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = ALERT_FROM
    msg["To"] = ALERT_TO
    msg.set_content(body)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


# ---------------- KAFKA BROKER ----------------

def check_kafka():
    admin = None

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=10000
        )

        topics = admin.list_topics()

        required = {"orders", "payments", "clickstream"}
        missing = required - set(topics)

        if missing:
            return False, f"Missing Kafka topics: {sorted(missing)}"

        return True, "Kafka broker reachable; all topics exist"

    except Exception as e:
        return False, str(e)

    finally:
        if admin:
            admin.close()


# ---------------- CONSUMER PROCESS ----------------

def check_consumer():
    try:
        for process in psutil.process_iter(["pid", "cmdline"]):
            cmdline = process.info.get("cmdline") or []

            if any("consumer.py" in str(arg).lower() for arg in cmdline):
                return True, f"Consumer running (PID {process.info['pid']})"

        return False, "Kafka-to-S3 consumer process not running"

    except Exception as e:
        return False, str(e)


# ---------------- S3 FRESHNESS ----------------

def check_s3():
    results = []
    client = boto3.client("s3")
    now = datetime.now(timezone.utc)

    for topic in TOPICS:
        prefix = f"raw/streaming/{topic}/"
        latest = None

        try:
            paginator = client.get_paginator("list_objects_v2")

            for page in paginator.paginate(
                Bucket=BUCKET,
                Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    if latest is None or obj["LastModified"] > latest:
                        latest = obj["LastModified"]

            if latest is None:
                results.append(
                    (False, topic, "No streaming objects found")
                )
                continue

            age = (now - latest).total_seconds() / 60

            if age > S3_MAX_AGE_MINUTES:
                results.append((
                    False,
                    topic,
                    f"Latest S3 event is {age:.1f} minutes old"
                ))
            else:
                results.append((
                    True,
                    topic,
                    f"Latest S3 event {age:.1f} minutes old"
                ))

        except Exception as e:
            results.append((False, topic, str(e)))

    return results


# ---------------- DATABRICKS WORKFLOW ----------------

def check_databricks():
    if not all([
        DATABRICKS_HOST,
        DATABRICKS_TOKEN,
        DATABRICKS_JOB_ID
    ]):
        return False, "Databricks host, token or job ID not configured"

    url = (
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/list"
        f"?job_id={DATABRICKS_JOB_ID}&limit=1"
    )

    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}"
        }
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            data = json.loads(response.read().decode())

        runs = data.get("runs", [])

        if not runs:
            return False, "No Databricks workflow runs found"

        run = runs[0]
        state = run.get("state", {})

        life_cycle = state.get("life_cycle_state", "UNKNOWN")
        result = state.get("result_state", "UNKNOWN")
        run_id = run.get("run_id")

        start_time = run.get("start_time")

        if start_time:
            age = (
                datetime.now(timezone.utc).timestamp()
                - start_time / 1000
            ) / 60

            if age > JOB_MAX_AGE_MINUTES:
                return False, (
                    f"Latest workflow run {run_id} is "
                    f"{age:.1f} minutes old"
                )

        if life_cycle in ["PENDING", "QUEUED", "RUNNING", "TERMINATING"]:
            return True, f"Workflow run {run_id} is {life_cycle}"

        if life_cycle == "TERMINATED" and result == "SUCCESS":
            return True, f"Workflow run {run_id} succeeded"

        return False, (
            f"Workflow run {run_id}: "
            f"{life_cycle} / {result}"
        )

    except urllib.error.HTTPError as e:
        return False, f"Databricks API HTTP error: {e.code}"

    except Exception as e:
        return False, str(e)


# ---------------- MAIN MONITOR ----------------

def run_monitor():

    checks = {}

    kafka_ok, kafka_msg = check_kafka()
    checks["Kafka Broker"] = (kafka_ok, kafka_msg)

    consumer_ok, consumer_msg = check_consumer()
    checks["Kafka Consumer"] = (consumer_ok, consumer_msg)

    for ok, topic, message in check_s3():
        checks[f"S3 {topic}"] = (ok, message)

    db_ok, db_msg = check_databricks()
    checks["Databricks Workflow"] = (db_ok, db_msg)

    failed = [
        f"{name}: {message}"
        for name, (ok, message) in checks.items()
        if not ok
    ]

    overall = "FAIL" if failed else "PASS"

    report = [
        f"Overall status: {overall}",
        f"Checked at: {datetime.now(timezone.utc).isoformat()}",
        ""
    ]

    for name, (ok, message) in checks.items():
        status = "PASS" if ok else "FAIL"
        report.append(f"{status} | {name} | {message}")

    report_text = "\n".join(report)

    print(report_text)
    logging.info(report_text)

    state = load_state()
    previous = state.get("status")

    if overall != previous:
        subject = f"E-commerce Lakehouse: {overall}"

        try:
            send_email(subject, report_text)
        except Exception as e:
            logging.exception("Email alert failed: %s", e)

    state["status"] = overall
    state["last_checked"] = datetime.now(timezone.utc).isoformat()
    state["checks"] = {
        name: {
            "status": "PASS" if ok else "FAIL",
            "details": message
        }
        for name, (ok, message) in checks.items()
    }

    save_state(state)

    return overall


if __name__ == "__main__":
    run_monitor()
