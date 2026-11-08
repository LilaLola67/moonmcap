"""Reproduce a radar clip and prefix recovery on the pinned public driving recording."""
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

from large_source_workflows import sha256
from mcap.exceptions import EndOfFile
from mcap.reader import NonSeekingReader

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "/radar/tracks"


def rows(stream):
    for schema, channel, message in NonSeekingReader(stream, validate_crcs=True).iter_messages(log_time_order=False):
        yield (channel.topic, message.log_time, message.publish_time, message.sequence, message.data,
               channel.message_encoding, (schema.name, schema.encoding, schema.data) if schema else None)


def read(path):
    with path.open("rb") as stream:
        return list(rows(stream))


def main():
    manifest = json.loads((ROOT / "examples/validation/public-corpus.json").read_text(encoding="utf-8"))
    item = next(entry for entry in manifest["files"] if entry["name"] == "driving-real.mcap")
    source = ROOT / "_interop/public-corpus" / item["name"]
    if not source.exists():
        subprocess.run([sys.executable, "tools/corpus_workflows.py", "--public"], cwd=ROOT, check=True)
    assert source.stat().st_size == item["bytes"] and sha256(source) == item["sha256"], "source fingerprint mismatch"
    parent = ROOT / "_interop/driving-example"
    parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(dir=parent))
    print(f"Driving workflow output: {work.relative_to(ROOT)}", flush=True)

    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js", "--deny-warn"], cwd=ROOT, check=True)
    subprocess.run([sys.executable, "tools/ros1_workflows.py", "--public"], cwd=ROOT, check=True)

    def run(entry, *args, status=0):
        script = ROOT / f"_build/js/debug/build/cmd/{entry}/{entry}.js"
        result = subprocess.run([os.environ.get("NODE", "node"), str(script), *map(str, args)], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", timeout=180)
        assert result.returncode == status, result.stderr or result.stdout
        return json.loads(result.stdout), result.stderr

    original = read(source)
    valid, _ = run("diagnose", "validate", source)
    assert valid["valid"] and int(valid["observed_messages"]) == len(original) == 1606
    times = sorted({row[1] for row in original if row[0] == TOPIC})
    middle = len(times) // 2
    assert middle >= 2 and len(times) >= middle + 3
    start, end = times[middle - 2], times[middle + 2]
    selected = sorted((row for row in original if row[0] == TOPIC and start <= row[1] <= end), key=lambda row: row[1])
    options = ("--topic", TOPIC, "--start", start, "--end", end)
    query, _ = run("mcap", "query", source, "--full", *options)
    queried = [(m["topic"], int(m["log_time"]), int(m["publish_time"]), m["sequence"], bytes.fromhex(m["data_hex"]))
               for m in query["messages"]]
    assert queried == [row[:5] for row in selected]
    clip = work / "radar-clip.mcap"
    run("mcap", "slice", source, *options, "--compression", "lz4", "--output", clip)
    assert run("diagnose", "validate", clip)[0]["valid"]
    assert read(clip) == selected

    data = source.read_bytes()
    chunks = []
    offset = 8
    while offset < len(data) - 8:
        opcode, length = struct.unpack_from("<BQ", data, offset)
        if opcode == 6:
            chunks.append((offset, length))
        offset += 9 + length
    chunk_offset, chunk_size = chunks[len(chunks) // 2]
    cutoff = chunk_offset + 9 + chunk_size // 2
    damaged = work / "derived-truncated.mcap"
    damaged.write_bytes(data[:cutoff])
    assert not run("diagnose", "validate", damaged, status=1)[0]["valid"]

    # Stop the independent oracle at the last complete record boundary.
    prefix = []
    try:
        prefix.extend(rows(io.BytesIO(data[:chunk_offset])))
    except EndOfFile:
        pass
    assert 0 < len(prefix) < len(original) and prefix == original[:len(prefix)]
    recovered = work / "recovered-prefix.mcap"
    _, stderr = run("mcap", "recover", damaged, "--compression", "zstd", "--output", recovered)
    recovery = json.loads(stderr)
    assert not recovery["complete"] and recovery["recovered_messages"] == len(prefix)
    assert run("diagnose", "validate", recovered)[0]["valid"]
    assert read(recovered) == prefix
    assert sha256(source) == item["sha256"]
    report = {"result": "passed", "source": item, "source_messages": len(original),
              "ros1_reference_comparisons": len(original), "source_data_crc_verified": valid["data_crc_verified"],
              "topic": TOPIC, "start_ns": str(start), "end_ns": str(end), "clip_messages": len(selected),
              "recovered_prefix_messages": len(prefix), "derived_truncation_bytes": cutoff,
              "original_unchanged": True, "artifacts": {p.name: sha256(p) for p in (clip, damaged, recovered)}}
    (work / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"PASS {len(original)} ROS 1 messages, {len(selected)} radar messages, {len(prefix)} recovered prefix messages")


if __name__ == "__main__":
    main()
