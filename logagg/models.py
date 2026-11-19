"""Log record model and parsing of raw input (JSON dicts or plain text lines)."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone

LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40, "FATAL": 50}
_LEVEL_ALIASES = {"WARNING": "WARN", "CRITICAL": "FATAL", "ERR": "ERROR", "TRACE": "DEBUG"}
MAX_MESSAGE_LEN = 8192

# e.g. 2026-11-11T10:00:00Z ERROR [payment-service] Timeout calling gateway
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)\s+"
    r"(?P<level>[A-Za-z]+)\s+\[?(?P<service>[\w.\-]+)\]?\s*[:\-]?\s*(?P<msg>.*)$"
)


@dataclass
class LogEntry:
    id: int
    ts: float          # epoch seconds (UTC)
    service: str
    level: str
    host: str
    message: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ts": self.ts,
            "time": datetime.fromtimestamp(self.ts, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "service": self.service,
            "level": self.level,
            "host": self.host,
            "message": self.message,
        }

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(d["id"], d["ts"], d["service"], d["level"], d.get("host", ""), d["message"])


def parse_ts(value) -> float:
    """Accept epoch seconds/millis, numeric strings, or ISO-8601 strings."""
    if isinstance(value, (int, float)):
        v = float(value)
    elif isinstance(value, str):
        s = value.strip()
        try:
            v = float(s)
        except ValueError:
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(f"bad timestamp: {value!r}") from exc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
    else:
        raise ValueError(f"bad timestamp: {value!r}")
    return v / 1000.0 if v > 1e11 else v  # looks like milliseconds


def normalize_level(level) -> str:
    lv = str(level or "INFO").strip().upper()
    lv = _LEVEL_ALIASES.get(lv, lv)
    return lv if lv in LEVELS else "INFO"


def parse_record(raw, now: float | None = None) -> LogEntry:
    """Turn a dict or a text line into a LogEntry (id is assigned later). Raises ValueError."""
    now = now if now is not None else time.time()
    if isinstance(raw, str):
        line = raw.strip()
        if not line:
            raise ValueError("empty line")
        m = _LINE_RE.match(line)
        if m and (m["level"].upper() in LEVELS or m["level"].upper() in _LEVEL_ALIASES):
            return LogEntry(0, parse_ts(m["ts"]), m["service"].lower(), normalize_level(m["level"]),
                            "", (m["msg"] or "")[:MAX_MESSAGE_LEN])
        return LogEntry(0, now, "unknown", "INFO", "", line[:MAX_MESSAGE_LEN])
    if isinstance(raw, dict):
        msg = raw.get("message", raw.get("msg"))
        if msg is None or not str(msg).strip():
            raise ValueError("missing 'message'")
        ts_raw = raw.get("ts", raw.get("timestamp", raw.get("time")))
        return LogEntry(
            0,
            parse_ts(ts_raw) if ts_raw is not None else now,
            str(raw.get("service", raw.get("app", "unknown"))).strip().lower() or "unknown",
            normalize_level(raw.get("level", raw.get("severity"))),
            str(raw.get("host", "")).strip().lower(),
            str(msg)[:MAX_MESSAGE_LEN],
        )
    raise ValueError(f"unsupported record type: {type(raw).__name__}")
