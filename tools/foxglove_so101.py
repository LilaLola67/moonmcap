"""Prepare and verify a local SO-101 comparison clip with TF context."""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from so101_workflows import messages

ROOT = Path(__file__).resolve().parents[1]
TOPICS = ("/joint_states", "/tf", "/tf_static")


def main():
    expected = json.loads((ROOT / "examples/so101/expected-summary.json").read_text(encoding="utf-8"))
    source = ROOT / "_interop/public-corpus/so101-real.mcap"
    if not source.exists():
        subprocess.run([sys.executable, "examples/so101/run.py"], cwd=ROOT, check=True)
    original = source.read_bytes()
    assert hashlib.sha256(original).hexdigest() == expected["source_sha256"]
    start, end = int(expected["start"]), int(expected["end"])
    rows = messages(original)
    selected = [row for row in rows if row[0] in TOPICS and start <= row[1] <= end]
    previous_static = [row for row in rows if row[0] == "/tf_static" and row[1] < start]
    assert previous_static, "recording has no prior static transform"
    context = max(previous_static, key=lambda row: row[1])
    parent = ROOT / "_interop/foxglove-so101"
    parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(dir=parent))
    config = {"start": str(start), "end": str(end), "topics": list(TOPICS),
              "context": [{"topic": "/tf_static", "depth": 1, "required": True}]}
    config_path = work / "clip.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js", "--deny-warn"],
                   cwd=ROOT, check=True, capture_output=True, timeout=600)
    node = os.environ.get("NODE", "node")
    output = work / "so101-joints-and-tf.mcap"
    cli = ROOT / "_build/js/debug/build/cmd/incident/incident.js"
    subprocess.run([node, str(cli), "clip", str(source), str(config_path), str(output)],
                   cwd=ROOT, check=True, capture_output=True, timeout=180)
    data = output.read_bytes()
    actual = messages(data)
    assert actual == [context] + sorted(selected, key=lambda row: row[1])
    counts = dict(Counter(row[0] for row in actual))
    assert counts["/joint_states"] == expected["clip_messages"] and counts["/tf"] > 0
    assert source.read_bytes() == original
    report = {"result": "file_validation_passed", "source_sha256": expected["source_sha256"],
              "clip_sha256": hashlib.sha256(data).hexdigest(), "start_ns": str(start), "end_ns": str(end),
              "context_log_time_ns": str(context[1]), "topic_counts": counts,
              "reference": "Python MCAP CRC-checked readback and exact payload comparison",
              "desktop_verification": "pending"}
    (work / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(work.relative_to(ROOT))
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
