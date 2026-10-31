"""Measure selected JS host bridges with V8 while running real failure workflows."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit
from urllib.request import url2pathname
import uuid

ROOT = Path(__file__).resolve().parents[1]
BRIDGES = {
    "atomic_bytes": "output published",
    "publish_stage": "staging output changed",
    "create_exclusive": "if(error&&key)",
    "append_bytes": "output offset exceeds Node safe range",
    "managed_open": "not a regular file",
    "managed_read": "managed source changed during read",
    "managed_close": "s.closed=true",
    "range_bytes": "source changed since opening",
    "recording_host": "MoonMCAP recording session",
    "artifact_host": "MoonMCAP artifact batch",
    "http_range": "HTTP representation must use identity content encoding",
    "worker_open": "node:worker_threads",
    "cache_open": "MoonMCAP HTTP cache",
}
WORKFLOWS = ["fs_fault_workflows", "http_workflows", "worker_workflows", "cached_http_workflows"]


def main():
    work = ROOT / "_interop/host-coverage" / uuid.uuid4().hex
    raw = work / "v8"
    raw.mkdir(parents=True)
    env = {**os.environ, "NODE_V8_COVERAGE": str(raw), "PYTHONUTF8": "1"}
    for name in WORKFLOWS:
        run_env = env.copy()
        if name == "worker_workflows":
            # Keep the original large-body deadline tests uninstrumented. V8
            # tracing substantially changes decoder runtime; a separate small
            # real-worker probe below measures the host bridge itself.
            run_env.pop("NODE_V8_COVERAGE", None)
        with (work / (name + ".log")).open("wb") as log:
            result = subprocess.run([sys.executable, "tools/" + name + ".py"], cwd=ROOT,
                                    env=run_env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
        assert result.returncode == 0, "host workflow failed; inspect " + str((work / (name + ".log")).relative_to(ROOT))
    probe = subprocess.run(["node", str(ROOT / "_build/js/debug/build/cmd/parallel/parallel.js"),
                            str(ROOT / "examples/files/zstd-summary.mcap"), "--limit", "1", "--min-chunk-bytes", "0",
                            "--worker-timeout", "15000"], cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", timeout=90)
    assert probe.returncode == 0 and len(probe.stdout.splitlines()) == 1, "instrumented worker entry probe failed"
    assert json.loads(probe.stderr)["pool"]["submitted"] > 0, "worker probe bypassed the worker"
    cache, artifacts = {}, {}
    results = {name: {"observed_function_calls": 0, "copies": set(), "positive_ranges": set(), "zero_observations": set()}
               for name in BRIDGES}
    profiles = list(raw.glob("*.json"))
    assert profiles, "Node did not emit V8 coverage"
    for profile in profiles:
        data = json.loads(profile.read_text(encoding="utf-8"))
        for script in data["result"]:
            parsed = urlsplit(script["url"])
            if parsed.scheme != "file":
                continue
            path = Path(url2pathname(parsed.path)).resolve()
            if not path.is_relative_to((ROOT / "_build").resolve()) or not path.is_file():
                continue
            if path not in cache:
                body = path.read_bytes()
                cache[path] = body.decode("utf-8").encode("utf-16-le")
                artifacts[path.relative_to(ROOT).as_posix()] = hashlib.sha256(body).hexdigest()
            utf16 = cache[path]
            for function in script["functions"]:
                name = function["functionName"].replace("__", "_")
                selected = [key for key in BRIDGES if name.endswith(key)]
                if not selected:
                    continue
                key = selected[0]
                bounds = function["ranges"][0]
                start, end = bounds["startOffset"], bounds["endOffset"]
                body = utf16[start * 2:end * 2].decode("utf-16-le")
                assert BRIDGES[key] in body, "V8 bridge mapping changed: " + key
                digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
                row = results[key]
                row["copies"].add(digest)
                row["observed_function_calls"] += bounds["count"]
                for item in function["ranges"][1:]:
                    relative = (digest, item["startOffset"] - start, item["endOffset"] - start)
                    row["positive_ranges" if item["count"] else "zero_observations"].add(relative)
    required = {"atomic_bytes", "publish_stage", "recording_host", "artifact_host", "http_range", "worker_open", "cache_open"}
    assert all(results[name]["observed_function_calls"] > 0 for name in required), "required host bridge was not exercised"
    rows = []
    for name, result in results.items():
        rows.append({"bridge": name, "executed": result["observed_function_calls"] > 0,
                     "observed_function_calls": result["observed_function_calls"], "body_sha256": sorted(result["copies"]),
                     "positive_compiled_ranges": len(result["positive_ranges"]),
                     "zero_count_observations": len(result["zero_observations"]),
                     "review_candidates": [{"body_sha256": digest, "utf16_start": start, "utf16_end": end}
                                           for digest, start, end in sorted(result["zero_observations"] - result["positive_ranges"])[:12]]})
    report = {"result": "passed", "measurement": "Node V8 compiled-JavaScript function and block execution evidence",
              "node": subprocess.check_output(["node", "--version"], text=True).strip(), "profiles": len(profiles),
              "workflows": WORKFLOWS, "bridges": rows, "compiled_artifacts": artifacts,
              "uninstrumented_deadline_workflow": "worker_workflows", "instrumented_worker_probe_timeout_ms": 15000,
              "limits": ["Not a source-level branch coverage percentage or an all-path proof.",
                         "V8 range shapes can differ by execution; zero observations are review candidates, not proof a source branch never ran.",
                         "Killed processes may not flush profiles; workflow assertions still verify those fault outcomes.",
                         "MoonBit unit line instrumentation is reported separately; the measurements are not added together."]}
    output = json.dumps(report, indent=2) + "\n"
    (work / "report.json").write_text(output, encoding="utf-8")
    (ROOT / "_interop/host-coverage/report.json").write_text(output, encoding="utf-8")
    print("Host coverage passed:", sum(row["executed"] for row in rows), "of", len(rows), "selected bridges executed")


if __name__ == "__main__":
    main()
