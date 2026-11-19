"""Segment = a self-contained inverted index over a set of log entries.

The active, mutable segment is the "memtable". When it grows past a threshold it is
persisted as an immutable, gzip-compressed segment file (LSM-tree style).
"""
from __future__ import annotations

import gzip
import json
import math
import os
from collections import Counter, defaultdict

from .models import LogEntry
from .query import Clause
from .tokenizer import tokenize

FIELD_NAMES = ("service", "level", "host")


class Segment:
    def __init__(self, seg_id: int = 0):
        self.id = seg_id
        self.docs: dict[int, LogEntry] = {}
        self.postings: dict[str, dict[int, int]] = defaultdict(dict)  # term -> {doc_id: term_freq}
        self.by_field: dict[str, dict[str, set]] = {f: defaultdict(set) for f in FIELD_NAMES}
        self.min_ts = float("inf")
        self.max_ts = float("-inf")
        self.max_id = 0

    def __len__(self) -> int:
        return len(self.docs)

    # ---------------------------------------------------------------- indexing
    def add(self, e: LogEntry) -> None:
        self.docs[e.id] = e
        for term, tf in Counter(tokenize(e.message)).items():
            self.postings[term][e.id] = tf
        self.by_field["service"][e.service].add(e.id)
        self.by_field["level"][e.level].add(e.id)
        self.by_field["host"][e.host].add(e.id)
        self.min_ts = min(self.min_ts, e.ts)
        self.max_ts = max(self.max_ts, e.ts)
        self.max_id = max(self.max_id, e.id)

    # ---------------------------------------------------------------- searching
    def overlaps(self, t_from, t_to) -> bool:
        """Segment pruning: skip whole segments outside the requested time window."""
        if not self.docs:
            return False
        if t_from is not None and self.max_ts < t_from:
            return False
        if t_to is not None and self.min_ts > t_to:
            return False
        return True

    def _idf(self, df: int) -> float:
        return math.log(1 + len(self.docs) / df)

    def _clause_scores(self, c: Clause) -> dict[int, float]:
        if c.field:
            return {i: 0.0 for i in self.by_field[c.field].get(c.value, ())}
        if c.prefix:
            scores: dict[int, float] = defaultdict(float)
            for term, plist in self.postings.items():
                if term.startswith(c.value):
                    idf = self._idf(len(plist))
                    for i, tf in plist.items():
                        scores[i] += tf * idf
            return scores
        terms = tokenize(c.value)
        if not terms:
            return {}
        result: dict[int, float] | None = None
        for t in terms:
            plist = self.postings.get(t)
            if not plist:
                return {}
            idf = self._idf(len(plist))
            cur = {i: tf * idf for i, tf in plist.items()}
            result = cur if result is None else {i: result[i] + cur[i] for i in result.keys() & cur.keys()}
            if not result:
                return {}
        if c.phrase and len(terms) > 1:
            needle = c.value.lower()
            result = {i: s for i, s in result.items() if needle in self.docs[i].message.lower()}
        return result or {}

    def _eval_group(self, group: list[Clause]) -> dict[int, float]:
        positives = [c for c in group if not c.negate]
        negatives = [c for c in group if c.negate]
        if positives:
            acc: dict[int, float] | None = None
            for c in positives:
                r = self._clause_scores(c)
                acc = r if acc is None else {i: acc[i] + r[i] for i in acc.keys() & r.keys()}
                if not acc:
                    return {}
        else:
            acc = {i: 0.0 for i in self.docs}   # only exclusions (or empty query): start from everything
        for c in negatives:
            for i in self._clause_scores(c):
                acc.pop(i, None)
        return acc

    def search(self, groups: list[list[Clause]], t_from=None, t_to=None) -> list[tuple[LogEntry, float]]:
        merged: dict[int, float] = {}
        for g in groups:
            for i, s in self._eval_group(g).items():
                if s > merged.get(i, -1.0):
                    merged[i] = s
        out = []
        for i, s in merged.items():
            e = self.docs[i]
            if (t_from is not None and e.ts < t_from) or (t_to is not None and e.ts > t_to):
                continue
            out.append((e, s))
        return out

    # ---------------------------------------------------------------- persistence
    def save(self, path: str) -> None:
        payload = {
            "id": self.id,
            "docs": [e.to_dict() for e in self.docs.values()],
            "postings": {t: {str(i): tf for i, tf in pl.items()} for t, pl in self.postings.items()},
        }
        tmp = path + ".tmp"
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, path)   # atomic: a crash never leaves a half-written segment

    @staticmethod
    def load(path: str) -> "Segment":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        seg = Segment(data["id"])
        for d in data["docs"]:
            e = LogEntry.from_dict(d)
            seg.docs[e.id] = e
            seg.by_field["service"][e.service].add(e.id)
            seg.by_field["level"][e.level].add(e.id)
            seg.by_field["host"][e.host].add(e.id)
            seg.min_ts = min(seg.min_ts, e.ts)
            seg.max_ts = max(seg.max_ts, e.ts)
            seg.max_id = max(seg.max_id, e.id)
        for t, pl in data["postings"].items():
            seg.postings[t] = {int(i): tf for i, tf in pl.items()}   # postings are restored, not rebuilt
        return seg

    @staticmethod
    def merge(seg_id: int, segments: list["Segment"]) -> "Segment":
        merged = Segment(seg_id)
        for e in sorted((e for s in segments for e in s.docs.values()), key=lambda x: x.id):
            merged.add(e)
        return merged
