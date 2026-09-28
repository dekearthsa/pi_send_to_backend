#!/usr/bin/env python3
"""
Tongdy PGX -> MQTT -> app.py (Pi) -> HTTP POST -> API Gateway -> Lambda -> DynamoDB (tongdy_pgx)

Usage
    python3 app.py               subscribe and forward every reading to the backend
    python3 app.py --dry-run     subscribe and only log the parsed records (nothing is sent)
                                 -> set TEST_MODE = True (settings below) to make this the default
    python3 app.py pending       show readings that failed to send (kept in a local SQLite file)
    python3 app.py resend        send those readings again, oldest first

Settings come from environment variables or a .env file next to this script (see .env.example).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import queue
import re
import signal
import socket
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent

try:  # optional .env support (pip install python-dotenv)
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass


def _path_setting(name: str, default: str) -> Path:
    path = Path(os.getenv(name) or default).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


# --- settings -----------------------------------------------------------------
# True  = test: only log the parsed readings, send nothing to the cloud (resend is skipped too)
# False = normal: forward every reading to API Gateway
TEST_MODE = True

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1884"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "/device/sensor/#")  # '#' also matches /device/sensor itself
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID") or f"tongdy-forwarder-{socket.gethostname()}"

API_URL = os.getenv(
    "API_URL",
    ".../kun-kran/tongdy/insert",
)
API_KEY = os.getenv("API_KEY") or None  # sent as the x-api-key header when set
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
HTTP_RETRIES = max(1, int(os.getenv("HTTP_RETRIES", "3")))

FAILED_DB = _path_setting("FAILED_DB", "failed_records.db")
RESEND_BATCH = max(1, min(int(os.getenv("RESEND_BATCH", "25")), 100))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# DID -> control (Subscribe) topic configured on each PGX web page
DEVICES: Dict[str, str] = {
    "PGX-42800030": "/device/control/01",
    "PGX-42800015": "/device/control/02",
}

MIN_VALID_TS = 1577836800  # 2020-01-01 UTC; anything older means the device clock is not set
RESERVED_KEYS = {"systemID", "timestamp", "received_at", "project_name", "inserted_at", "TL"}
_UNSAFE_KEY_CHARS = re.compile(r"[^0-9A-Za-z_]")

log = logging.getLogger("tongdy")


# --- Tongdy payload -> record ---------------------------------------------------
def clean_key(name: Any) -> str:
    """'PM2.5' -> 'PM2_5' so DynamoDB attribute names stay easy to query."""
    return _UNSAFE_KEY_CHARS.sub("_", str(name).strip()) or "unknown"


def to_epoch_seconds(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(ts):
        return None
    if ts > 1e12:  # milliseconds
        ts /= 1000
    return int(ts)


def parse_tongdy(payload: Any, received_at: Optional[int] = None) -> Dict[str, Any]:
    """
    {"DID":"PGX-42800030","TL":-1,"sensor":[{"2":"timestamp","3":1790568438},{"2":"T","3":27.07},...]}
    -> {"systemID":"PGX-42800030","timestamp":1790568438,"received_at":...,"TL":-1,"T":27.07,...}
    """
    if not isinstance(payload, dict):
        raise ValueError("payload is not a JSON object")
    did = payload.get("DID")
    if not isinstance(did, str) or not did.strip():
        raise ValueError("payload has no DID")

    now = int(time.time()) if received_at is None else int(received_at)
    record: Dict[str, Any] = {"systemID": did.strip(), "timestamp": now, "received_at": now}
    if payload.get("TL") is not None:
        record["TL"] = payload["TL"]

    device_ts: Any = None
    for item in payload.get("sensor") or []:
        if not isinstance(item, dict) or "2" not in item:
            continue
        name, value = item.get("2"), item.get("3")
        if name == "timestamp":
            device_ts = value
            continue
        if value is None or (isinstance(value, float) and not math.isfinite(value)):
            continue
        key = clean_key(name)
        if key in RESERVED_KEYS:
            key = "s_" + key
        record[key] = value

    ts = to_epoch_seconds(device_ts)
    if ts is not None and ts >= MIN_VALID_TS:
        record["timestamp"] = ts
    else:
        log.warning("%s: device timestamp %r is not usable, using Pi time %s", record["systemID"], device_ts, now)
    return record


# --- HTTP -----------------------------------------------------------------------
def _retryable(status: Optional[int]) -> bool:
    """Network errors, throttling and server errors are worth retrying; other 4xx are not."""
    return status is None or status == 429 or status >= 500


def post_json(session: Any, body: Any) -> Tuple[Optional[int], str]:
    """POST body to API_URL. Returns (HTTP status, or None on a network error; error text or '')."""
    import requests

    try:
        data = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        return 0, f"cannot encode record: {exc}"
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["x-api-key"] = API_KEY
    try:
        resp = session.post(API_URL, data=data, headers=headers, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if 200 <= resp.status_code < 300:
        return resp.status_code, ""
    return resp.status_code, f"HTTP {resp.status_code}: {resp.text[:300]}"


def send_with_retry(session: Any, body: Any) -> Tuple[bool, Optional[int], str]:
    status: Optional[int] = None
    error = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        status, error = post_json(session, body)
        if not error:
            return True, status, ""
        if not _retryable(status) or attempt == HTTP_RETRIES:
            break
        time.sleep(min(2 ** (attempt - 1), 8))
    return False, status, error


# --- local store for readings that could not be sent -----------------------------
class FailedStore:
    """SQLite file that keeps unsent readings until `python3 app.py resend`."""

    def __init__(self, path: Path) -> None:
        self.path = path
        with self._db() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS failed_records (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    system_id  TEXT,
                    ts         INTEGER,
                    body       TEXT    NOT NULL,
                    error      TEXT,
                    attempts   INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL
                )
                """
            )

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        con = sqlite3.connect(str(self.path), timeout=30)
        try:
            with con:  # commit on success, roll back on error
                yield con
        finally:
            con.close()

    def add(self, record: Dict[str, Any], error: str) -> None:
        with self._db() as con:
            con.execute(
                "INSERT INTO failed_records (system_id, ts, body, error, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    record.get("systemID"),
                    record.get("timestamp"),
                    json.dumps(record, ensure_ascii=False),
                    error[:500],
                    int(time.time()),
                ),
            )

    def count(self) -> int:
        with self._db() as con:
            return con.execute("SELECT COUNT(*) FROM failed_records").fetchone()[0]

    def summary(self) -> Tuple[int, Optional[int], Optional[int], Optional[str]]:
        with self._db() as con:
            count, first, last = con.execute(
                "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM failed_records"
            ).fetchone()
            row = con.execute("SELECT error FROM failed_records ORDER BY id DESC LIMIT 1").fetchone()
        return count, first, last, (row[0] if row else None)

    def fetch_after(self, last_id: int, limit: int) -> List[Tuple[int, Dict[str, Any]]]:
        with self._db() as con:
            rows = con.execute(
                "SELECT id, body FROM failed_records WHERE id > ? ORDER BY id LIMIT ?", (last_id, limit)
            ).fetchall()
        return [(row_id, json.loads(body)) for row_id, body in rows]

    def delete(self, ids: List[int]) -> None:
        with self._db() as con:
            con.executemany("DELETE FROM failed_records WHERE id = ?", [(i,) for i in ids])

    def mark_failed(self, ids: List[int], error: str) -> None:
        with self._db() as con:
            con.executemany(
                "UPDATE failed_records SET attempts = attempts + 1, error = ? WHERE id = ?",
                [(error[:500], i) for i in ids],
            )


# --- sender thread (keeps slow HTTP calls off the MQTT network loop) --------------
class Forwarder(threading.Thread):
    def __init__(self, store: Optional[FailedStore], dry_run: bool = False) -> None:
        super().__init__(name="forwarder", daemon=True)
        self.store = store
        self.dry_run = dry_run
        self.queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=10000)

    def submit(self, record: Dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self._save(record, "local send queue full")

    def run(self) -> None:
        session = None
        if not self.dry_run:
            import requests

            session = requests.Session()
        while True:
            record = self.queue.get()
            if record is None:
                return
            try:
                if self.dry_run:
                    log.info("[TEST_MODE] %s", json.dumps(record, ensure_ascii=False))
                    continue
                ok, status, error = send_with_retry(session, record)
                if ok:
                    log.info(
                        "sent %s ts=%s T=%s HUM=%s Co2=%s (HTTP %s)",
                        record["systemID"], record["timestamp"],
                        record.get("T"), record.get("HUM"), record.get("Co2"), status,
                    )
                else:
                    self._save(record, error)
            except Exception as exc:  # never let the sender thread die
                log.exception("unexpected error while sending %s", record.get("systemID"))
                self._save(record, f"{type(exc).__name__}: {exc}")

    def _save(self, record: Dict[str, Any], error: str) -> None:
        if self.store is None:
            log.error("dropped %s ts=%s: %s", record.get("systemID"), record.get("timestamp"), error)
            return
        try:
            self.store.add(record, error)
            log.warning(
                "send failed for %s ts=%s -> kept for resend (%s)",
                record.get("systemID"), record.get("timestamp"), error,
            )
        except Exception:
            log.exception("could not store failed record: %s", json.dumps(record, ensure_ascii=False))

    def stop(self, timeout: float = 30.0) -> None:
        try:
            self.queue.put(None, timeout=5)
        except queue.Full:
            pass
        self.join(timeout)
        while True:  # anything still queued is kept for resend
            try:
                record = self.queue.get_nowait()
            except queue.Empty:
                break
            if record is not None:
                self._save(record, "stopped before sending")


# --- commands ---------------------------------------------------------------------
def run(dry_run: bool = False) -> int:
    import paho.mqtt.client as mqtt

    forwarder = Forwarder(None if dry_run else FailedStore(FAILED_DB), dry_run=dry_run)
    forwarder.start()

    # Persistent session (clean_session=False + QoS 1): the broker queues readings while app.py is down.
    # A dry run uses its own throwaway session so it never consumes readings meant for the real forwarder.
    client_id = f"{MQTT_CLIENT_ID}-dryrun" if dry_run else MQTT_CLIENT_ID
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=dry_run)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.reconnect_delay_set(min_delay=1, max_delay=60)

    def on_connect(client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        if reason_code.is_failure:
            log.error("MQTT connection refused: %s", reason_code)
            return
        log.info("MQTT connected to %s:%s, subscribing %s", MQTT_HOST, MQTT_PORT, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)

    def on_disconnect(client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        if reason_code != 0:
            log.warning("MQTT disconnected (%s), reconnecting...", reason_code)

    def on_message(client: Any, userdata: Any, msg: Any) -> None:
        try:
            record = parse_tongdy(json.loads(msg.payload.decode("utf-8")), int(time.time()))
        except (UnicodeDecodeError, ValueError) as exc:  # json.JSONDecodeError is a ValueError
            log.warning("skipped message on %s: %s | %r", msg.topic, exc, msg.payload[:200])
            return
        if record["systemID"] not in DEVICES:
            log.warning("unknown device %s on %s (add it to DEVICES)", record["systemID"], msg.topic)
        log.debug("received %s", record)
        forwarder.submit(record)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    def _terminate(signum: int, frame: Any) -> None:  # systemd stop -> same path as Ctrl+C
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _terminate)

    log.info(
        "MQTT %s:%s topic %s -> %s",
        MQTT_HOST, MQTT_PORT, MQTT_TOPIC, "log only" if dry_run else API_URL,
    )
    if dry_run:
        log.warning("TEST_MODE is ON: readings are only logged, nothing is sent to the cloud")
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    try:
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        log.info("stopping...")
    finally:
        client.disconnect()
        forwarder.stop()
    return 0


def pending() -> int:
    count, first, last, last_error = FailedStore(FAILED_DB).summary()
    if not count:
        print("No readings waiting for resend.")
        return 0

    def fmt(ts: Optional[int]) -> str:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"

    print(f"{count} reading(s) waiting in {FAILED_DB}")
    print(f"  failed between {fmt(first)} and {fmt(last)}")
    print(f"  last error: {last_error}")
    print("Run `python3 app.py resend` to send them.")
    return 0


def resend(test_mode: bool = False) -> int:
    store = FailedStore(FAILED_DB)
    total = store.count()
    if not total:
        print("Nothing to resend.")
        return 0

    if test_mode:  # only show what would be sent; keep everything in the DB
        last_id = 0
        while True:
            rows = store.fetch_after(last_id, RESEND_BATCH)
            if not rows:
                break
            last_id = rows[-1][0]
            for row_id, record in rows:
                log.info("[TEST_MODE] would resend id=%s %s", row_id, json.dumps(record, ensure_ascii=False))
        print(f"TEST_MODE is ON: {total} reading(s) only logged, nothing sent. Set TEST_MODE = False to resend.")
        return 0

    import requests

    session = requests.Session()
    sent = rejected = 0
    last_id = 0
    while True:
        rows = store.fetch_after(last_id, RESEND_BATCH)
        if not rows:
            break
        last_id = rows[-1][0]
        ids = [row_id for row_id, _ in rows]

        ok, status, error = send_with_retry(session, {"records": [record for _, record in rows]})
        if ok:
            store.delete(ids)
            sent += len(ids)
        elif status in (400, 413, 422):
            # the backend rejected the batch: send one by one so a single bad reading does not block the rest
            for row_id, record in rows:
                ok1, status1, error1 = send_with_retry(session, record)
                if ok1:
                    store.delete([row_id])
                    sent += 1
                elif status1 in (400, 413, 422):
                    store.mark_failed([row_id], error1)
                    rejected += 1
                    print(f"  rejected id={row_id} {record.get('systemID')} ts={record.get('timestamp')}: {error1}")
                else:
                    store.mark_failed([row_id], error1)
                    print(f"Stopped: {error1}. Sent {sent}/{total}, the rest stay queued.")
                    return 1
        else:
            store.mark_failed(ids, error)
            print(f"Stopped: {error}. Sent {sent}/{total}, the rest stay queued.")
            return 1
        print(f"  sent {sent}/{total}")

    if rejected:
        print(f"Done: sent {sent}, rejected {rejected} (still in the DB, see `python3 app.py pending`).")
        return 2
    print(f"Done: sent {sent}.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Forward Tongdy PGX MQTT readings to the kun_kran backend.")
    parser.add_argument("command", nargs="?", default="run", choices=["run", "pending", "resend"])
    parser.add_argument("--dry-run", action="store_true", help="only log parsed records, send nothing (same as TEST_MODE = True)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    test_mode = TEST_MODE or args.dry_run
    if args.command == "pending":
        return pending()
    if args.command == "resend":
        return resend(test_mode)
    return run(dry_run=test_mode)


if __name__ == "__main__":
    sys.exit(main())
