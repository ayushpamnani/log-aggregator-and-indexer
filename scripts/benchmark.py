"""Quick benchmark: ingest N synthetic logs, then time some queries.   python scripts/benchmark.py 50000"""
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from logagg.agent import synth  # noqa: E402
from logagg.engine import Engine  # noqa: E402

n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
d = tempfile.mkdtemp()
eng = Engine(d, flush_threshold=5000, queue_size=20000)
eng.start()
t0 = time.time()
for i in range(0, n, 500):
    eng.ingest([synth(time.time()) for _ in range(500)])
eng.wait_idle(60)
dt = time.time() - t0
print(f"ingested+indexed {n} logs in {dt:.2f}s  ({n / dt:,.0f} logs/s)")
for q in ["timeout", "level:error service:payment-service", '"connection refused"', "pay* -healthcheck", ""]:
    r = eng.search(q, limit=20)
    print(f"  q={q!r:45} total={r['total']:>6}  {r['took_ms']:>8} ms")
eng.stop()
shutil.rmtree(d, ignore_errors=True)
