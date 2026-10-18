"""Local HTTP fault server: range/validator correctness, retry, timeout and abort."""
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    work = ROOT / "_interop/http"
    work.mkdir(parents=True, exist_ok=True)
    data = (ROOT / "examples/files/zstd-summary.mcap").read_bytes()
    counts = defaultdict(int)
    trace = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def handle(self):
            try:
                super().handle()
            except ConnectionError:
                pass  # The client deliberately aborts several faulty responses.

        def do_GET(self):
            with lock:
                counts[self.path] += 1
                call = counts[self.path]
            mode = self.path.lstrip("/")
            if mode == "timeout" or (mode == "cancel" and call > 1):
                time.sleep(0.25)
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            if not match:
                self.send_error(400)
                return
            start, end = map(int, match.groups())
            if mode == "retry" and call == 1:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if mode == "changed" and call > 1:
                self.send_response(412)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if mode == "redirect":
                self.send_response(302)
                self.send_header("Location", "/ok")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            payload = data[start:end + 1]
            status = 200 if mode == "ignore" else 206
            if mode == "ignore":
                payload = data
            self.send_response(status)
            total = len(data) + (1 if mode == "size-change" and call > 1 else 0)
            span_start = start + 1 if mode == "bad-range" else start
            self.send_header("Content-Range", f"bytes {span_start}-{end}/{total}")
            etag = 'W/"v1"' if mode == "weak" else ('"v2"' if mode == "etag-change" and call > 1 else '"v1"')
            self.send_header("ETag", etag)
            if mode == "encoding":
                self.send_header("Content-Encoding", "gzip")
            if mode == "oversize":
                payload += b"extra"
                self.send_header("Connection", "close")
                self.close_connection = True
            else:
                self.send_header("Content-Length", "999999999" if mode == "bad-length" else str(len(payload)))
            if mode == "short":
                payload = payload[:-1]
                self.close_connection = True
            self.end_headers()
            try:
                self.wfile.write(payload)
            except ConnectionError:
                pass
            with lock:
                trace.append({"mode": mode, "start": start, "end": end, "if_match": self.headers.get("If-Match"), "bytes": len(payload)})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/async/async.js"
    base = f"http://127.0.0.1:{server.server_port}"
    rows = []
    try:
        for mode in ("ok", "retry", "changed", "etag-change", "size-change", "ignore", "weak", "bad-range", "bad-length", "oversize", "short", "encoding", "timeout", "redirect"):
            retries = "1" if mode == "retry" else "0"
            command = [os.environ.get("NODE", "node"), str(cli), "remote", base + "/" + mode,
                       "/imu", "20000000000", "30000000000", "30" if mode == "timeout" else "2000", retries]
            run = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=15)
            if mode in ("ok", "retry"):
                assert run.returncode == 0, run.stderr
                report = json.loads(run.stdout)
                assert [int(m["log_time"]) // 10**9 for m in report["messages"]] == [21, 23, 25, 27, 29]
                if mode == "retry":
                    assert counts["/retry"] >= counts["/ok"] + 1
            else:
                assert run.returncode == 2, mode + run.stdout
            rows.append({"case": mode, "result": "passed", "requests": counts["/" + mode]})
        cancel = subprocess.run([os.environ.get("NODE", "node"), str(cli), "remote-cancel", base + "/cancel"], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", timeout=10)
        assert cancel.returncode == 0, cancel.stderr
        assert json.loads(cancel.stdout) == {"cancelled": True, "read_aborted": True}
        rows.append({"case": "abort-inflight-range", "result": "passed"})
        successful = [t for t in trace if t["mode"] == "ok"]
        assert successful and all(t["if_match"] == '"v1"' for t in successful[1:])
        assert sum(t["bytes"] for t in successful) < len(data)
        (work / "report.json").write_text(json.dumps({"cases": rows, "successful_ranges": successful}, indent=2) + "\n", encoding="utf-8")
        print(f"HTTP PASS: {len(rows)} range, snapshot, retry, timeout, body-limit and cancellation workflows")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
