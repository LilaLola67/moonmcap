"""Reproducible legacy/direct/managed file IO comparison; timing is observational."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    work = ROOT / "_interop/io"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    path = work / "bytes.bin"
    data = bytes(range(256)) * 16384 + b"tail"
    path.write_bytes(data)
    cli = ROOT / "_build/js/debug/build/cmd/io/io.js"
    reports = []
    for mode in ("legacy", "direct", "managed"):
        samples = []
        for _ in range(3):
            result = subprocess.run([os.environ.get("NODE", "node"), str(cli), mode, str(path)], cwd=ROOT, capture_output=True, text=True, timeout=90)
            assert result.returncode == 0, result.stderr
            report = json.loads(result.stdout)
            assert report["sha256"] == hashlib.sha256(data).hexdigest()
            assert report["cached_bytes"] <= 65536
            samples.append(report)
        samples.sort(key=lambda x: x["elapsed_ms"])
        reports.append(samples[1])
    assert len({r["checksum"] for r in reports}) == 1
    assert reports[2]["physical_reads"] < reports[2]["logical_reads"] // 8
    (work / "report.json").write_text(json.dumps({"timing_is_observational": True, "bytes": len(data), "median_of": 3, "samples": reports}, indent=2)+"\n")
    for report in reports:
        print(f"{report['mode']}: {report['elapsed_ms']:.1f} ms, {report['physical_reads']} physical reads, {report['peak_rss_kib']} KiB peak RSS")


if __name__ == "__main__":
    main()
