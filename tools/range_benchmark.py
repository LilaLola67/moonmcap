"""Reproducible selective-I/O check; byte counters are not process RSS."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / "_interop/range"
    work.mkdir(parents=True, exist_ok=True)
    data = io.BytesIO()
    writer = Writer(data, compression=CompressionType.NONE, chunk_size=128 * 1024,
                    enable_crcs=True, enable_data_crcs=True)
    writer.start(library="official-python-selective-io-fixture")
    channel = writer.register_channel("/imu", "binary", 0)
    for i in range(2048):
        writer.add_message(channel, i, bytes([i % 251]) * 8192, i, sequence=i)
    writer.finish()
    source = work / "large.mcap"
    source.write_bytes(data.getvalue())
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT,
                   check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/range/range.js"
    started = time.perf_counter()
    run = subprocess.run([os.environ.get("NODE", "node"), str(cli), str(source), "/imu", "1000", "1009"],
                         cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8")
    report = json.loads(run.stdout)
    assert report["messages"] == 10
    assert report["times"] == [str(i) for i in range(1000, 1010)]
    assert int(report["bytes_read"]) < source.stat().st_size // 10
    assert report["cache_hits"] > 0 and not report["full_validation"]
    assert report["peak_cache_bytes"] <= 8 * 1024 * 1024
    report.update(result="passed", fixture_messages=2048, payload_bytes=8192,
                  elapsed_seconds=round(time.perf_counter() - started, 4),
                  note="Elapsed time includes Node startup and two queries. Cache bytes count decompressed payload, not process RSS.")
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Selective I/O PASS: {report['bytes_read']} / {report['source_bytes']} bytes, {report['decoded_chunks']} decoded chunks, {report['cache_hits']} cache hits")


if __name__ == "__main__":
    main()
