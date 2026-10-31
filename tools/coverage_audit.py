"""Generate local coverage evidence and a risk-file inventory; no percentage quality gate."""

import json, os, subprocess, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RISK = [
    "artifact_batch.mbt",
    "recording_session.mbt",
    "range_reader.mbt",
    "stream_writer.mbt",
    "sidecar.mbt",
    "cli_options.mbt",
    "source_scan.mbt",
    "asyncio/dataset.mbt",
    "asyncio/writer.mbt",
    "asyncio/pipeline.mbt",
    "asyncio/http.mbt",
    "asyncio/worker_pool.mbt",
    "asyncio/cached.mbt",
    "nodeio/files.mbt",
    "nodeio/storage.mbt",
]


def main():
    work = ROOT / "_interop/coverage"
    work.mkdir(parents=True, exist_ok=True)
    moon = os.environ.get("MOON", "moon")
    # Each measurement must use matching instrumentation and counters, never
    # stale outputs from an earlier source tree or compiler.
    target = work / ("build-" + uuid.uuid4().hex)
    for name, args in [
        ("test", ["test", "--target", "js", "--enable-coverage", '--deny-warn']),
        (
            "report",
            ["coverage", "report", "-f", "coveralls", "-o", str(work / "raw.json")],
        ),
    ]:
        if name == "test":
            args += ["--target-dir", str(target)]
        else:
            sources = sorted(target.rglob("*.trace.source"))
            counters = sorted(target.glob("moonbit_coverage_*.txt"))
            assert sources and counters, "fresh instrumentation or counters missing"
            combined = work / "runtime.txt"
            combined.write_bytes(b"\n".join(p.read_bytes() for p in counters))
            args += ["--runtime-log", str(combined), *map(str, sources)]
        with (work / (name + ".log")).open("w", encoding="utf-8") as log:
            r = subprocess.run(
                [moon, *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT
            )
        if r.returncode:
            raise RuntimeError(
                "Coverage " + name + " failed; inspect " + str(work / (name + ".log"))
            )
    data = json.loads((work / "raw.json").read_text(encoding="utf-8"))
    rows = []
    for f in data["source_files"]:
        name = f["name"].replace("\\", "/")
        if name not in RISK:
            continue
        c = f["coverage"]
        rows.append(
            {
                "file": name,
                "covered_lines": sum(v is not None and v > 0 for v in c),
                "executable_lines": sum(v is not None for v in c),
                "uncovered_lines": [i + 1 for i, v in enumerate(c) if v == 0],
            }
        )
    assert {r["file"] for r in rows} == set(RISK)
    report = {
        "result": "passed",
        "measurement": "MoonBit JS unit-test line instrumentation only",
        "risk_files": rows,
        "selected_regressions": ["risk_paths_test.mbt", "asyncio/cleanup_test.mbt"],
        "limitations": [
            "Not branch coverage.",
            "Does not measure embedded JavaScript host bodies.",
            "External CLI workflows are not merged into this trace.",
            "Uncovered lines remain candidates for review, not proof of defects.",
        ],
        "percentage_gate": False,
    }
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Coverage risk inventory generated:", len(rows), "files; no percentage gate")


if __name__ == "__main__":
    main()
