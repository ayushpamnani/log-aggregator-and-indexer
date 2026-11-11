"""Engine: ties together ingestion (WAL + queue), the background indexer, segments and search.

Write path:  parse -> assign id -> WAL append (durable) -> bounded queue -> indexer thread -> memtable
Flush:       memtable -> immutable segment file -> WAL truncated
Background:  compaction (merge small segments) + retention (drop old segments)
Read path:   parse query -> prune segments by time -> search memtable + segments -> merge -> rank -> page
"""
from __future__ import annotations

import glob
import os
import queue
import threading
import time
from collections import Counter

from .index import Segment
from .models import LogEntry, parse_record
from .query import parse_query
from .wal import WAL

MAX_BATCH = 1000


class Backpressure(Exception):
    """Ingest queue is full; the client should retry later (HTTP 429)."""


class Engine:
    def __init__(self, data_dir: str = "data", flush_threshold: int = 2000, queue_size: int = 10000,
                 retention_seconds: float | None = None, max_segments: int = 6, wal_fsync: bool = False):
        self.data_dir = data_dir
        self.seg_dir = os.path.join(data_dir, "segments")
        os.makedirs(self.seg_dir, exist_ok=True)
        self.flush_threshold = flush_threshold
        self.retention_seconds = retention_seconds
        self.max_segments = max_segments

        self.lock = threading.RLock()          # guards memtable + segment list
        self.ingest_lock = threading.Lock()    # serialises id assignment + WAL append + enqueue
        self.queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.memtable = Segment(0)
        self.segments: list[Segment] = []
        self.next_id = 1
        self.next_seg_id = 1
        self.wal = WAL(os.path.join(data_dir, "wal.log"), fsync=wal_fsync)
        self.started_at = time.time()
        self.metrics = Counter()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._recover()

    # ------------------------------------------------------------------ lifecycle
    def _recover(self) -> None:
        for path in sorted(glob.glob(os.path.join(self.seg_dir, "seg-*.json.gz"))):
            seg = Segment.load(path)
            self.segments.append(seg)
            self.next_seg_id = max(self.next_seg_id, seg.id + 1)
            self.next_id = max(self.next_id, seg.max_id + 1)
        flushed_max = max((s.max_id for s in self.segments), default=0)
        replayed = 0
        for e in self.wal.replay():
            if e.id > flushed_max:
                self.memtable.add(e)
                self.next_id = max(self.next_id, e.id + 1)
                replayed += 1
        self.metrics["wal_replayed"] = replayed

    def start(self) -> None:
        self._thread = threading.Thread(target=self._indexer_loop, name="indexer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.wal.close()

    # ------------------------------------------------------------------ ingestion
    def ingest(self, records: list) -> dict:
        if len(records) > MAX_BATCH:
            raise ValueError(f"batch too large (max {MAX_BATCH})")
        entries, errors = [], []
        for i, raw in enumerate(records):
            try:
                entries.append(parse_record(raw))
            except ValueError as exc:
                errors.append({"index": i, "error": str(exc)})
        self.metrics["rejected_invalid"] += len(errors)
        if entries:
            with self.ingest_lock:
                free = self.queue.maxsize - self.queue.qsize()
                if len(entries) > free:
                    self.metrics["rejected_backpressure"] += len(entries)
                    raise Backpressure("ingest queue full")
                for e in entries:
                    e.id = self.next_id
                    self.next_id += 1
                self.wal.append(entries)            # durable before we acknowledge
                for e in entries:
                    self.queue.put_nowait(e)
            self.metrics["ingested"] += len(entries)
        return {"accepted": len(entries), "rejected": len(errors), "errors": errors[:10]}

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """Block until everything queued has been indexed (used by tests / demos)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.queue.unfinished_tasks == 0:
                return True
            time.sleep(0.01)
        return False

    # ------------------------------------------------------------------ indexer thread
    def _indexer_loop(self) -> None:
        last_maint = time.time()
        while not (self._stop.is_set() and self.queue.empty()):
            try:
                batch = [self.queue.get(timeout=0.25)]
            except queue.Empty:
                if time.time() - last_maint > 30:
                    self._maintenance()
                    last_maint = time.time()
                continue
            while len(batch) < 500:
                try:
                    batch.append(self.queue.get_nowait())
                except queue.Empty:
                    break
            with self.lock:
                for e in batch:
                    self.memtable.add(e)
            self.metrics["indexed"] += len(batch)
            for _ in batch:
                self.queue.task_done()
            if len(self.memtable) >= self.flush_threshold:
                self.flush()

    def flush(self) -> None:
        with self.lock:
            if not len(self.memtable):
                return
            seg = self.memtable
            seg.id = self.next_seg_id
            self.next_seg_id += 1
            seg.save(self._seg_path(seg.id))
            self.segments.append(seg)
            self.memtable = Segment(0)
            flushed_max = seg.max_id
        self.wal.truncate_upto(flushed_max)
        self.metrics["flushes"] += 1
        self._maintenance()

    def _seg_path(self, seg_id: int) -> str:
        return os.path.join(self.seg_dir, f"seg-{seg_id:06d}.json.gz")

    def _maintenance(self) -> None:
        self.apply_retention()
        self.compact()

    def apply_retention(self) -> int:
        if not self.retention_seconds:
            return 0
        cutoff = time.time() - self.retention_seconds
        with self.lock:
            expired = [s for s in self.segments if s.max_ts < cutoff]
            self.segments = [s for s in self.segments if s.max_ts >= cutoff]
        for s in expired:
            self._remove_file(self._seg_path(s.id))
        self.metrics["segments_expired"] += len(expired)
        return len(expired)

    def compact(self) -> bool:
        """Size-tiered compaction: merge the smallest segments to bound the segment count."""
        with self.lock:
            if len(self.segments) <= self.max_segments:
                return False
            n = len(self.segments) - self.max_segments + 1
            victims = sorted(self.segments, key=len)[:max(n, 2)]
            merged = Segment.merge(self.next_seg_id, victims)
            self.next_seg_id += 1
            merged.save(self._seg_path(merged.id))
            self.segments = [s for s in self.segments if s not in victims] + [merged]
        for s in victims:
            self._remove_file(self._seg_path(s.id))
        self.metrics["compactions"] += 1
        return True

    @staticmethod
    def _remove_file(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass

    # ------------------------------------------------------------------ search
    def search(self, q: str = "", t_from: float | None = None, t_to: float | None = None,
               limit: int = 50, offset: int = 0, sort: str = "time") -> dict:
        start = time.perf_counter()
        groups = parse_query(q)
        with self.lock:
            targets = [self.memtable] + list(self.segments)
            hits, searched, skipped = [], 0, 0
            for seg in targets:
                if not seg.overlaps(t_from, t_to):
                    skipped += 1
                    continue
                searched += 1
                hits.extend(seg.search(groups, t_from, t_to))
        if sort == "relevance":
            hits.sort(key=lambda h: (-h[1], -h[0].ts))
        else:
            hits.sort(key=lambda h: (-h[0].ts, -h[0].id))
        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        page = [dict(h[0].to_dict(), score=round(h[1], 3)) for h in hits[offset:offset + limit]]
        self.metrics["queries"] += 1
        return {
            "query": q,
            "total": len(hits),
            "took_ms": round((time.perf_counter() - start) * 1000, 2),
            "segments_searched": searched,
            "segments_skipped": skipped,
            "facets": {
                "level": dict(Counter(h[0].level for h in hits)),
                "service": dict(Counter(h[0].service for h in hits).most_common(10)),
            },
            "results": page,
        }

    # ------------------------------------------------------------------ stats
    def stats(self) -> dict:
        with self.lock:
            seg_docs = sum(len(s) for s in self.segments)
            return {
                "uptime_s": round(time.time() - self.started_at, 1),
                "documents": {"total_indexed": seg_docs + len(self.memtable),
                              "in_memtable": len(self.memtable), "in_segments": seg_docs},
                "segments": [{"id": s.id, "docs": len(s), "terms": len(s.postings)} for s in self.segments],
                "queue_depth": self.queue.qsize(),
                "queue_capacity": self.queue.maxsize,
                "wal_bytes": self.wal.size(),
                "counters": dict(self.metrics),
            }
