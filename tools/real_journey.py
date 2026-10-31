"""Pinned original recording: validate, query, clip, derive truncation, recover independently."""

import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import NonSeekingReader
from mcap.stream_reader import StreamReader


def row(channel, message):
    return (
        channel.topic,
        message.log_time,
        message.publish_time,
        message.sequence,
        message.data,
    )


def read(data):
    return [
        row(c, m)
        for _, c, m in NonSeekingReader(
            io.BytesIO(data), validate_crcs=True
        ).iter_messages(log_time_order=False)
    ]


def main():
    item = json.loads(
        (ROOT / "examples/validation/public-corpus.json").read_text(encoding="utf-8")
    )["files"][0]
    source = ROOT / "_interop/public-corpus" / item["name"]
    source.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        # Download to memory and check before saving; no silently accepted replacement.
        with urllib.request.urlopen(item["url"], timeout=120) as response:
            data = response.read(item["bytes"] + 1)
        assert (
            len(data) == item["bytes"]
            and hashlib.sha256(data).hexdigest() == item["sha256"]
        )
        source.write_bytes(data)
    data = source.read_bytes()
    assert (
        len(data) == item["bytes"]
        and hashlib.sha256(data).hexdigest() == item["sha256"]
    )
    work = ROOT / "_interop/real-journey" / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run(
        [os.environ.get("MOON", "moon"), "build", "--target", "js"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    node = os.environ.get("NODE", "node")

    def command(entry, *values, status=0):
        result = subprocess.run(
            [
                node,
                str(ROOT / f"_build/js/debug/build/cmd/{entry}/{entry}.js"),
                *map(str, values),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
        )
        assert result.returncode == status, result.stdout + result.stderr
        return json.loads(result.stdout)

    validation = command("diagnose", "validate", source)
    assert validation["valid"]
    oracle = read(data)
    topic = oracle[0][0]
    eligible = sorted((r for r in oracle if r[0] == topic), key=lambda r: r[1])
    assert len(eligible) > 20
    start, end = eligible[5][1], eligible[18][1]
    expected = [r for r in eligible if start <= r[1] <= end]
    options = ["--topic", topic, "--start", start, "--end", end, "--limit", 100000]
    query = command("mcap", "query", source, *options)
    actual = [
        (
            m["topic"],
            int(m["log_time"]),
            int(m["publish_time"]),
            m["sequence"],
            bytes.fromhex(m["data_hex"]),
        )
        for m in query["messages"]
    ]
    assert actual == expected
    clip = work / "clip.mcap"
    command("mcap", "slice", source, *options, "--output", clip)
    assert read(clip.read_bytes()) == expected
    list(StreamReader(io.BytesIO(clip.read_bytes()), validate_crcs=True).records)
    chunks = []
    position = 8
    while position < len(data) - 8:
        opcode, length = struct.unpack_from("<BQ", data, position)
        if opcode == 6:
            chunks.append((position, length))
        position += 9 + length
    assert len(chunks) >= 2
    position, length = chunks[len(chunks) // 2]
    cutoff = position + 9 + length // 2
    truncated = data[:cutoff]
    damaged = work / "derived-truncated.mcap"
    damaged.write_bytes(truncated)
    prefix = []
    failed = False
    try:
        for _, c, m in NonSeekingReader(
            io.BytesIO(truncated), validate_crcs=True
        ).iter_messages(log_time_order=False):
            prefix.append(row(c, m))
    except Exception:
        failed = True
    assert failed and 0 < len(prefix) < len(oracle)
    invalid = command("diagnose", "validate", damaged, status=1)
    assert not invalid["valid"]
    recovered = work / "recovered.mcap"
    # Prefix recovery accepts a truncated container; indexed block recovery requires
    # an intact summary and is deliberately not used for this damaged layout.
    recovery = command("mcap", "recover", damaged, "--output", recovered)
    assert read(recovered.read_bytes()) == prefix
    list(StreamReader(io.BytesIO(recovered.read_bytes()), validate_crcs=True).records)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == item["sha256"]
    assert damaged.read_bytes() == truncated
    report = {
        "result": "passed",
        "original": item,
        "messages": len(oracle),
        "query": {
            "topic": topic,
            "start": str(start),
            "end": str(end),
            "messages": len(expected),
        },
        "clip_independently_equal": True,
        "derived_fault": {
            "kind": "artificial truncation of original public recording",
            "cutoff_bytes": cutoff,
            "interrupted_chunk_offset": position,
            "sha256": hashlib.sha256(truncated).hexdigest(),
            "recovered_prefix_messages": len(prefix),
            "not_an_original_recording_failure": True,
        },
        "recovery": recovery,
        "original_unchanged": True,
        "reference": "mcap 1.3.0 NonSeekingReader and StreamReader with CRC validation",
        "limits": [
            "No raw public data is redistributed.",
            "Recovered valid prefix does not prove completeness or quantify unread lost messages.",
        ],
    }
    output = json.dumps(report, indent=2) + "\n"
    (work / "report.json").write_text(output, encoding="utf-8")
    (ROOT / "_interop/real-journey/report.json").write_text(output, encoding="utf-8")
    print(
        f"Real journey passed: {len(oracle)} original messages, {len(expected)} clipped, {len(prefix)} recovered prefix"
    )


if __name__ == "__main__":
    main()
