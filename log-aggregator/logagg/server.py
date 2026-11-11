"""HTTP API (stdlib only).

POST /ingest   JSON object | JSON array | {"logs":[...]} | text/plain (one log per line)
GET  /search   ?q=...&from=...&to=...&limit=50&offset=0&sort=time|relevance
GET  /stats    engine metrics
GET  /health   liveness
GET  /         built-in search UI
"""
from __future__ import annotations

import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Backpressure, Engine
from .models import parse_ts

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
MAX_BODY = 5 * 1024 * 1024


def make_handler(engine: Engine, api_key: str | None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "logagg/1.0"

        def log_message(self, fmt, *args):  # keep the console quiet
            pass

        def _send(self, status: int, body, ctype: str = "application/json"):
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authorized(self) -> bool:
            return not api_key or self.headers.get("X-API-Key") == api_key

        def do_GET(self):
            url = urlparse(self.path)
            qs = parse_qs(url.query)
            one = lambda k, d=None: qs.get(k, [d])[0]
            try:
                if url.path == "/":
                    with open(os.path.join(WEB_DIR, "index.html"), "rb") as f:
                        return self._send(200, f.read(), "text/html; charset=utf-8")
                if url.path == "/health":
                    return self._send(200, {"status": "ok"})
                if url.path == "/stats":
                    return self._send(200, engine.stats())
                if url.path == "/search":
                    t_from = parse_ts(one("from")) if one("from") else None
                    t_to = parse_ts(one("to")) if one("to") else None
                    res = engine.search(one("q", ""), t_from, t_to, int(one("limit", 50)),
                                        int(one("offset", 0)), one("sort", "time"))
                    return self._send(200, res)
                return self._send(404, {"error": "not found"})
            except ValueError as exc:
                return self._send(400, {"error": str(exc)})

        def do_POST(self):
            if urlparse(self.path).path == "/flush":
                if not self._authorized():
                    return self._send(401, {"error": "invalid or missing X-API-Key"})
                engine.wait_idle()
                engine.flush()
                return self._send(200, {"status": "flushed"})
            if urlparse(self.path).path != "/ingest":
                return self._send(404, {"error": "not found"})
            if not self._authorized():
                return self._send(401, {"error": "invalid or missing X-API-Key"})
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY:
                return self._send(413 if length > MAX_BODY else 400, {"error": "bad body size"})
            body = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                if "json" in (self.headers.get("Content-Type") or ""):
                    payload = json.loads(body)
                    if isinstance(payload, dict) and "logs" in payload:
                        payload = payload["logs"]
                    records = payload if isinstance(payload, list) else [payload]
                else:
                    records = [ln for ln in body.splitlines() if ln.strip()]
                return self._send(202, engine.ingest(records))
            except Backpressure:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Type", "application/json")
                msg = b'{"error":"ingest queue full, retry later"}'
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            except ValueError as exc:   # includes json.JSONDecodeError
                self._send(400, {"error": str(exc)})

    return Handler


def main() -> None:
    p = argparse.ArgumentParser(description="Log aggregator & indexer server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--flush-threshold", type=int, default=2000, help="docs in memtable before flushing a segment")
    p.add_argument("--queue-size", type=int, default=10000)
    p.add_argument("--retention-hours", type=float, default=None, help="drop segments older than this")
    p.add_argument("--max-segments", type=int, default=6)
    p.add_argument("--fsync", action="store_true", help="fsync the WAL on every batch (slower, safer)")
    p.add_argument("--api-key", default=os.environ.get("LOGAGG_API_KEY"))
    a = p.parse_args()

    engine = Engine(a.data_dir, a.flush_threshold, a.queue_size,
                    a.retention_hours * 3600 if a.retention_hours else None, a.max_segments, a.fsync)
    engine.start()
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(engine, a.api_key))
    print(f"logagg listening on http://{a.host}:{a.port}  (data: {a.data_dir}, "
          f"recovered {engine.stats()['documents']['total_indexed']} docs)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        srv.server_close()
        engine.stop()


if __name__ == "__main__":
    main()
