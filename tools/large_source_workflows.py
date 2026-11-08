"""Opt-in actual million-message and >2 GiB files; no sparse-file simulation."""
import argparse
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType, IndexType
from mcap.reader import NonSeekingReader


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(data)
    return digest.hexdigest()


def compare(source, moon_command, expected):
    """Alternate process order and retain the raw samples, including startup."""
    commands = {
        "moonmcap": moon_command,
        "python_mcap": [sys.executable, str(ROOT / "tools/python_mcap_scan.py"), str(source)],
    }
    samples = {name: [] for name in commands}
    orders = []
    for order in (tuple(commands), tuple(reversed(commands))):
        orders.append(order)
        for name in order:
            started = time.perf_counter()
            run = subprocess.run(commands[name], cwd=ROOT, check=True, capture_output=True,
                                 text=True, encoding="utf-8", timeout=1800)
            elapsed = time.perf_counter() - started
            result = json.loads(run.stdout)
            if name == "moonmcap":
                assert result["valid"] and result["data_valid"]
                usage = json.loads(next(line.removeprefix("MOONMCAP_RSS=") for line in run.stderr.splitlines()
                                        if line.startswith("MOONMCAP_RSS=")))
                result = {"messages": int(result["observed_messages"]), "start": int(result["observed_time_start"]),
                          "end": int(result["observed_time_end"]), "data_crc_verified": result["data_crc_verified"],
                          "max_rss_kib": usage["max_rss_kib"]}
            assert {key: result[key] for key in expected} == expected
            samples[name].append({"seconds": round(elapsed, 4), "max_rss_kib": result["max_rss_kib"]})
            print(f"{source.stem} {name}: {elapsed:.2f}s, {result['max_rss_kib']} KiB", flush=True)
    return {"order": orders, "verified_output": expected, "samples": samples,
            "median_seconds": {name: statistics.median(row["seconds"] for row in runs) for name, runs in samples.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--large", action="store_true", help="write and verify actual >2 GiB and 1,000,000-message fixtures")
    parser.add_argument("--compare", action="store_true", help="also measure CRC-checked Python and MoonMCAP scans on the same inputs")
    args = parser.parse_args()
    work = ROOT / "_interop/large-source"
    if args.compare:
        work = ROOT / "_interop/large-source-comparison" / uuid.uuid4().hex
    work.mkdir(parents=True, exist_ok=True)
    if args.large and shutil.disk_usage(work).free < 3 * 1024**3:
        raise SystemExit("Large verification requires at least 3 GiB free disk space")
    moon = os.environ.get("MOON", "moon")
    subprocess.run([moon, "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/diagnose/diagnose.js"
    cases = [("million-messages", 1_000_000 if args.large else 10_000, 32, 512 * 1024),
             ("over-2gib", 2049 if args.large else 33, 1024 * 1024, 4 * 1024 * 1024)]
    rows = []
    for name, count, payload_size, chunk_size in cases:
        source = work / f"{name}.mcap"
        # Each path is fixed, owned by this script, and removed individually.
        try:
            with source.open("wb") as stream:
                writer = Writer(stream, compression=CompressionType.NONE, chunk_size=chunk_size,
                                index_types=IndexType.NONE, enable_crcs=True, enable_data_crcs=True)
                writer.start(library="official-python-mcap/1.3.0-scale-reference")
                channel = writer.register_channel("/scale", "raw", 0)
                payload = bytes(range(256)) * (payload_size // 256) if payload_size >= 256 else bytes(range(payload_size))
                for i in range(count):
                    writer.add_message(channel, i * 10, payload, i * 10, i)
                writer.finish()
            size = source.stat().st_size
            if args.large and name == "over-2gib":
                assert size > 2**31
            digest = sha256(source)
            started = time.perf_counter()
            moon_command = [os.environ.get("NODE", "node"), str(ROOT / "tools/rss_probe.cjs"), str(cli),
                            "validate", str(source), "--record-limit", "3000000"]
            result = subprocess.run(moon_command, cwd=ROOT,
                                    capture_output=True, text=True, encoding="utf-8", timeout=1800)
            elapsed = time.perf_counter() - started
            assert result.returncode == 0, result.stderr + result.stdout
            report = json.loads(result.stdout)
            usage = json.loads(next(line.removeprefix("MOONMCAP_RSS=") for line in result.stderr.splitlines() if line.startswith("MOONMCAP_RSS=")))
            assert report["valid"] and report["data_valid"] and report["data_crc_verified"]
            assert int(report["observed_messages"]) == count and int(report["source_bytes"]) == size
            assert report["io"]["peak_read_bytes"] <= 1024 * 1024 and report["io"]["peak_record_span"] <= 16 * 1024 * 1024 + 9
            assert report["observed_time_start"] == "0" and int(report["observed_time_end"]) == (count - 1) * 10
            with source.open("rb") as stream:
                observed = 0
                reader = NonSeekingReader(stream, validate_crcs=True)
                for _, channel, message in reader.iter_messages(log_time_order=False):
                    assert channel.topic == "/scale" and message.sequence == observed
                    assert message.data == payload and message.log_time == observed * 10
                    observed += 1
                assert observed == count
            assert sha256(source) == digest
            rows.append({"case": name, "messages": count, "source_bytes": size, "sha256": digest,
                         "elapsed_seconds": round(elapsed, 4), **usage, "diagnosis": report})
            print(f"{name}: {count} messages, {size} bytes, {usage['max_rss_kib']} KiB peak RSS, {elapsed:.2f}s", flush=True)
            if args.compare:
                rows[-1]["comparison"] = compare(source, moon_command, {
                    "messages": count, "start": 0, "end": (count - 1) * 10, "data_crc_verified": True,
                })
                assert sha256(source) == digest
        finally:
            source.unlink(missing_ok=True)
    result = {"large": args.large, "fixture": "official MCAP Python writer, actual bytes written to disk, CRCs enabled",
              "rss_note": "OS process high-water RSS includes runtime and temporary allocations; read/record budgets are not an RSS ceiling",
              "cases": rows}
    if args.compare:
        versions = subprocess.check_output([moon, "version", "--all"], cwd=ROOT, text=True, encoding="utf-8")
        compiler = next(line.split()[1] for line in versions.splitlines() if line.startswith("moonc "))
        result["comparison_environment"] = {
            "system": platform.system(), "machine": platform.machine(), "python": platform.python_version(),
            "python_mcap": version("mcap"), "moonc": compiler,
        }
        result["comparison_scope"] = (
            "Full uncompressed streaming scans with available DataEnd and Chunk CRCs. Both report message count "
            "and time bounds; MoonMCAP also performs structural diagnostics. Process startup is included. "
            "Inputs were already read before timing; OS cache state was not controlled. Fixture generation, "
            "SHA-256 and the independent per-message fixture oracle are outside timed runs."
        )
    (work / ("report-large.json" if args.large else "report.json")).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"large source workflows: {len(rows)} passed (large={args.large})")
    if args.compare:
        print(f"Comparison report: {work.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
