import os
import shutil
import tempfile
import time
import unittest

from logagg.engine import Backpressure, Engine
from logagg.models import parse_record, parse_ts
from logagg.query import parse_query
from logagg.tokenizer import tokenize


def rec(msg, service="svc", level="INFO", ts=None, host="h1"):
    return {"message": msg, "service": service, "level": level, "host": host, "ts": ts or time.time()}


class TestParsing(unittest.TestCase):
    def test_tokenize(self):
        self.assertEqual(tokenize("Timeout: gateway_1 (500)!"), ["timeout", "gateway_1", "500"])

    def test_parse_text_line(self):
        e = parse_record("2026-11-11T10:00:00Z ERROR [payment-service] Timeout calling gateway")
        self.assertEqual((e.level, e.service, e.message), ("ERROR", "payment-service", "Timeout calling gateway"))

    def test_parse_plain_line_fallback(self):
        e = parse_record("just some text")
        self.assertEqual((e.level, e.service), ("INFO", "unknown"))

    def test_parse_dict_and_timestamps(self):
        e = parse_record({"msg": "hi", "severity": "warning", "app": "A", "ts": 1_700_000_000_000})
        self.assertEqual((e.level, e.service, e.ts), ("WARN", "a", 1_700_000_000.0))
        self.assertEqual(parse_ts("2026-01-01T00:00:00Z"), parse_ts("2026-01-01T00:00:00+00:00"))

    def test_missing_message_rejected(self):
        with self.assertRaises(ValueError):
            parse_record({"service": "x"})

    def test_query_parser(self):
        g = parse_query('timeout level:error -healthcheck "conn refused" pay* OR service:auth')
        self.assertEqual(len(g), 2)
        c = g[0]
        self.assertEqual(c[1].field, "level"); self.assertEqual(c[1].value, "ERROR")
        self.assertTrue(c[2].negate); self.assertTrue(c[3].phrase); self.assertTrue(c[4].prefix)


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.eng = Engine(self.dir, flush_threshold=5, max_segments=3)
        self.eng.start()

    def tearDown(self):
        self.eng.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    def ingest(self, *records):
        out = self.eng.ingest(list(records))
        self.eng.wait_idle()
        return out

    def test_search_terms_and_filters(self):
        self.ingest(rec("timeout calling gateway", "pay", "ERROR"), rec("payment ok", "pay"),
                    rec("timeout in auth", "auth", "WARN"))
        self.assertEqual(self.eng.search("timeout")["total"], 2)
        self.assertEqual(self.eng.search("timeout level:error")["total"], 1)
        self.assertEqual(self.eng.search("timeout service:auth")["total"], 1)
        self.assertEqual(self.eng.search("timeout -gateway")["total"], 1)
        self.assertEqual(self.eng.search("payment OR gateway")["total"], 2)
        self.assertEqual(self.eng.search("time*")["total"], 2)
        self.assertEqual(self.eng.search("")["total"], 3)

    def test_phrase_query(self):
        self.ingest(rec("connection refused by db"), rec("refused connection by db"))
        self.assertEqual(self.eng.search('"connection refused"')["total"], 1)

    def test_time_range_and_pagination(self):
        now = time.time()
        self.ingest(*[rec(f"event {i}", ts=now - i * 100) for i in range(10)])
        self.assertEqual(self.eng.search("event", t_from=now - 450)["total"], 5)
        page = self.eng.search("event", limit=3, offset=3)
        self.assertEqual(len(page["results"]), 3)
        self.assertEqual(page["total"], 10)

    def test_flush_compaction_keeps_data(self):
        for i in range(8):
            self.ingest(*[rec(f"batch{i} item{j}") for j in range(5)])
        self.eng.wait_idle(); time.sleep(0.3)
        st = self.eng.stats()
        self.assertEqual(st["documents"]["total_indexed"], 40)
        self.assertLessEqual(len(st["segments"]), 3)
        self.assertEqual(self.eng.search("batch3")["total"], 5)

    def test_recovery_from_wal_and_segments(self):
        self.ingest(*[rec(f"persisted {i}") for i in range(7)])   # 5 flushed + 2 in memtable/WAL
        self.eng.stop()
        eng2 = Engine(self.dir, flush_threshold=5)
        try:
            self.assertEqual(eng2.search("persisted")["total"], 7)
            self.assertEqual(len({r["id"] for r in eng2.search("persisted")["results"]}), 7)
        finally:
            eng2.wal.close()
        self.eng = Engine(self.dir, flush_threshold=5)   # so tearDown can stop it
        self.eng.start()

    def test_retention(self):
        eng = Engine(tempfile.mkdtemp(), flush_threshold=2, retention_seconds=3600)
        eng.start()
        try:
            eng.ingest([rec("old a", ts=time.time() - 7200), rec("old b", ts=time.time() - 7200)])
            eng.wait_idle(); time.sleep(0.3)
            eng.apply_retention()
            self.assertEqual(eng.search("old")["total"], 0)
        finally:
            eng.stop()

    def test_backpressure_and_invalid(self):
        eng = Engine(tempfile.mkdtemp(), queue_size=3)   # indexer NOT started -> queue fills up
        eng.ingest([rec("a"), rec("b"), rec("c")])
        with self.assertRaises(Backpressure):
            eng.ingest([rec("d")])
        out = eng.ingest([{"nomessage": 1}])
        self.assertEqual((out["accepted"], out["rejected"]), (0, 1))
        eng.wal.close()


if __name__ == "__main__":
    unittest.main()
