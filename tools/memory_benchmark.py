"""Measure real child-process RSS separately from codec/run payload budgets."""
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader


def main():
    work = ROOT / "_interop/memory"
    temporary = work / "runs"
    temporary.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []
    for count in (512, 2048):
        sources = []
        for index in range(2):
            source = work / f"{count}-{index}.mcap"
            # Write directly to disk; the parent never holds the complete fixture.
            with source.open("wb") as stream:
                writer = Writer(stream, compression=CompressionType.NONE, chunk_size=128*1024,
                                enable_crcs=True, enable_data_crcs=True)
                writer.start()
                channel = writer.register_channel("/bench", "raw", 0)
                payload = random.Random(20261018 + index).randbytes(8192)
                for i in range(count):
                    writer.add_message(channel, i, payload, i, i)
                writer.finish()
            sources.append(source)
        cases = [("selective-query", ["query", str(sources[0]), "--start", "100", "--end", "109", "--limit", "10"])]
        output = work / f"merged-{count}.mcap"
        output.unlink(missing_ok=True)
        cases.append(("external-merge", ["external-merge", *map(str, sources), "--output", str(output),
                      "--temp-dir", str(temporary), "--run-bytes", "262144", "--merge-bytes", "262144", "--fan-in", "4"]))
        for name, args in cases:
            started = time.perf_counter()
            run = subprocess.run([os.environ.get("NODE", "node"), str(ROOT / "tools/rss_probe.cjs"), str(cli), *args],
                                 cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=180)
            assert run.returncode == 0, run.stderr
            usage = json.loads(next(line.removeprefix("MOONMCAP_RSS=") for line in run.stderr.splitlines() if line.startswith("MOONMCAP_RSS=")))
            assert usage["max_rss_kib"] > 0
            report = json.loads(run.stdout)
            if name == "selective-query":
                assert len(report["messages"]) == 10
                assert int(report["diagnostics"]["bytes_read"]) < sources[0].stat().st_size // 5
            else:
                assert report["peak_run_bytes"] <= 262144 and report["peak_merge_bytes"] <= 262144
                with output.open("rb") as stream:
                    assert sum(1 for _ in make_reader(stream, validate_crcs=True).iter_messages()) == count * 2
                assert not list(temporary.iterdir())
            rows.append({"case": name, "messages_per_input": count, "payload_bytes_per_message": 8192,
                         "input_files": 1 if name == "selective-query" else 2,
                         "total_input_bytes": sum(p.stat().st_size for p in (sources[:1] if name == "selective-query" else sources)), **usage,
                         "elapsed_seconds": round(time.perf_counter() - started, 4), "codec": report})
    toolchain = subprocess.run([os.environ.get("MOON", "moon"), "version", "--all"], capture_output=True, text=True).stdout.strip()
    result = {"toolchain": toolchain, "rss_source": "Node process.resourceUsage().maxRSS, OS process high-water mark in KiB",
              "note": "Run/pending/cache bytes are payload counters, not RSS caps. RSS includes JS runtime, GC, codecs and buffers. Timing is observational, not a performance threshold.", "cases": rows}
    (work / "report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("RSS workflows passed: " + ", ".join(f"{r['case']}/{r['messages_per_input']}: {r['max_rss_kib']} KiB" for r in rows))


if __name__ == "__main__":
    main()
