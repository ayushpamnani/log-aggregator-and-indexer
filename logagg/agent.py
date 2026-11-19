"""Log shipper (the "collector" in front of the aggregator).

  python -m logagg.agent --generate --rate 200          # synthetic traffic from fake services
  python -m logagg.agent --tail /var/log/app.log --service billing
Batches logs, retries with exponential backoff, and honours HTTP 429 backpressure.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import time
import urllib.error
import urllib.request

SERVICES = {
    "auth-service": [("INFO", "user {u} logged in from {ip}"), ("WARN", "failed login attempt for user {u} from {ip}"),
                     ("ERROR", "token verification failed for user {u}: signature mismatch")],
    "payment-service": [("INFO", "payment {n} completed amount={a}"), ("ERROR", "timeout calling payment gateway after 3000ms order={n}"),
                        ("ERROR", "connection refused by gateway.example.com order={n}"), ("WARN", "retrying payment {n} attempt=2")],
    "search-service": [("INFO", "query served in {ms}ms"), ("DEBUG", "cache miss for key q{n}"), ("WARN", "slow query took {ms}ms")],
    "inventory-service": [("INFO", "stock updated sku={n} qty={a}"), ("ERROR", "database deadlock detected on table stock"), ("INFO", "healthcheck ok")],
}


def synth(now: float) -> dict:
    service = random.choice(list(SERVICES))
    level, tpl = random.choice(SERVICES[service])
    msg = tpl.format(u=f"user{random.randint(1, 500)}", ip=f"10.0.{random.randint(0, 9)}.{random.randint(1, 250)}",
                     n=random.randint(1000, 9999), a=random.randint(1, 500), ms=random.randint(5, 4000))
    return {"ts": now, "service": service, "level": level, "message": msg,
            "host": f"node-{random.randint(1, 4)}"}


class Shipper:
    def __init__(self, url: str, api_key: str | None = None, batch_size: int = 200):
        self.url, self.api_key, self.batch_size = url.rstrip("/") + "/ingest", api_key, batch_size
        self.buf: list[dict] = []

    def add(self, rec: dict) -> None:
        self.buf.append(rec)
        if len(self.buf) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        data = json.dumps(self.buf).encode()
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        delay = 0.5
        for attempt in range(8):
            try:
                with urllib.request.urlopen(urllib.request.Request(self.url, data, headers), timeout=10):
                    self.buf = []
                    return
            except urllib.error.HTTPError as exc:
                if exc.code not in (429, 500, 502, 503):
                    print(f"[agent] dropping batch: HTTP {exc.code}")
                    self.buf = []
                    return
            except OSError:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 10)
        print("[agent] giving up on batch after 8 attempts")
        self.buf = []


def main() -> None:
    p = argparse.ArgumentParser(description="Log shipper")
    p.add_argument("--url", default="http://127.0.0.1:8080")
    p.add_argument("--api-key", default=os.environ.get("LOGAGG_API_KEY"))
    p.add_argument("--generate", action="store_true", help="send synthetic logs")
    p.add_argument("--rate", type=int, default=100, help="logs/second when generating")
    p.add_argument("--count", type=int, default=0, help="stop after N logs (0 = forever)")
    p.add_argument("--tail", help="path of a log file to follow")
    p.add_argument("--service", default="unknown")
    a = p.parse_args()
    shipper = Shipper(a.url, a.api_key)
    host = socket.gethostname().lower()
    sent = 0
    try:
        if a.generate:
            while not a.count or sent < a.count:
                t0 = time.time()
                for _ in range(max(1, a.rate // 10)):
                    shipper.add(synth(time.time()))
                    sent += 1
                shipper.flush()
                time.sleep(max(0.0, 0.1 - (time.time() - t0)))
        elif a.tail:
            with open(a.tail, "r", encoding="utf-8", errors="replace") as f:
                f.seek(0, os.SEEK_END)
                while True:
                    line = f.readline()
                    if line:
                        shipper.add({"message": line.rstrip("\n"), "service": a.service, "host": host})
                    else:
                        shipper.flush()
                        time.sleep(0.5)
        else:
            p.error("choose --generate or --tail FILE")
    except KeyboardInterrupt:
        pass
    finally:
        shipper.flush()
        print(f"[agent] sent {sent} logs")


if __name__ == "__main__":
    main()
