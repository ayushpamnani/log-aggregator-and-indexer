import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
import tempfile
import shutil

from logagg.engine import Engine
from logagg.server import make_handler


class TestServerAuth(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        self.engine = Engine(self.data_dir)
        self.engine.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.engine, "college-demo-key"))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.engine.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_flush_requires_api_key(self):
        request = urllib.request.Request(self.base + "/flush", data=b"", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        self.assertEqual(caught.exception.code, 401)

    def test_flush_accepts_valid_api_key(self):
        request = urllib.request.Request(
            self.base + "/flush", data=b"", headers={"X-API-Key": "college-demo-key"}, method="POST"
        )
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read()), {"status": "flushed"})


if __name__ == "__main__":
    unittest.main()
