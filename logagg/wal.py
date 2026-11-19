"""Write-ahead log: every accepted log is appended (as a JSON line) BEFORE it is acknowledged.

If the process crashes before the indexer flushes a segment, the WAL is replayed on startup.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Iterable, Iterator

from .models import LogEntry


class WAL:
    def __init__(self, path: str, fsync: bool = False):
        self.path = path
        self.fsync = fsync
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")

    def append(self, entries: Iterable[LogEntry]) -> None:
        with self._lock:
            for e in entries:
                self._f.write(json.dumps(e.to_dict(), separators=(",", ":")) + "\n")
            self._f.flush()
            if self.fsync:
                os.fsync(self._f.fileno())

    def replay(self) -> Iterator[LogEntry]:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    yield LogEntry.from_dict(json.loads(line))
                except (ValueError, KeyError):
                    continue   # torn write at the tail after a crash: ignore

    def truncate_upto(self, max_id: int) -> None:
        """Drop entries with id <= max_id (they are safely stored in a segment file)."""
        with self._lock:
            self._f.flush()
            keep = [e for e in self.replay() if e.id > max_id]
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for e in keep:
                    f.write(json.dumps(e.to_dict(), separators=(",", ":")) + "\n")
            self._f.close()
            os.replace(tmp, self.path)
            self._f = open(self.path, "a", encoding="utf-8")

    def size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except OSError:
            return 0

    def close(self) -> None:
        with self._lock:
            self._f.close()
