"""Run checked-in MCAP examples and independently verify their outputs."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import make_reader, NonSeekingReader
from mcap.stream_reader import StreamReader


def inspect(path):
    data = path.read_bytes()
    list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
    reader = make_reader(io.BytesIO(data), validate_crcs=True)
    messages = []
    for schema, channel, message in NonSeekingReader(io.BytesIO(data), validate_crcs=True).iter_messages():
        messages.append({"schema": (schema.name, schema.encoding, schema.data) if schema else None,
                         "topic": channel.topic, "encoding": channel.message_encoding,
                         "channel_metadata": channel.metadata, "sequence": message.sequence,
                         "log_time": message.log_time, "publish_time": message.publish_time,
                         "data": message.data})
    attachments = [(a.name, a.media_type, a.log_time, a.create_time, a.data)
                   for a in reader.iter_attachments()]
    metadata = [(m.name, m.metadata) for m in reader.iter_metadata()]
    summary = reader.get_summary()
    if summary and summary.statistics:
        assert summary.statistics.message_count == len(messages)
        assert summary.statistics.attachment_count == len(attachments)
        assert summary.statistics.metadata_count == len(metadata)
    return messages, attachments, metadata


def checked(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(result.stdout[-3000:] + result.stderr[-3000:])
    return result.stdout


def main():
    manifest = json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))
    for fixture in manifest["files"]:
        data = (HERE / fixture["path"]).read_bytes()
        assert len(data) == fixture["bytes"], fixture["path"]
        assert hashlib.sha256(data).hexdigest() == fixture["sha256"], fixture["path"]

    reference = inspect(HERE / "files/none-summary.mcap")
    assert len(reference[0]) == 100 and len(reference[1]) == len(reference[2]) == 1
    for fixture in manifest["files"]:
        if fixture["role"] == "input":
            assert inspect(HERE / fixture["path"]) == reference, fixture["path"]

    checked([os.environ.get("MOON", "moon"), "build", "--target", "js"])
    cli = ROOT / "_build/js/debug/build/cmd/interop/interop.js"
    work = ROOT / "_interop/examples"
    work.mkdir(parents=True, exist_ok=True)
    results = []
    for case in manifest["cases"]:
        output = work / Path(case["output"]).name
        command = [os.environ.get("NODE", "node"), str(cli), str(HERE / case["input"]),
                   str(output), case["compression"], case["operation"]]
        if "second_input" in case:
            command.append(str(HERE / case["second_input"]))
        stdout = checked(command)
        actual = inspect(output)
        if case["id"] == "slice":
            expected = ([m for m in reference[0] if m["topic"] == "/imu" and
                         20 * 10**9 <= m["log_time"] <= 30 * 10**9], reference[1], reference[2])
            assert [m["log_time"] // 10**9 for m in actual[0]] == [21, 23, 25, 27, 29]
        elif case["id"] == "merge":
            expected = (sorted(reference[0] + reference[0], key=lambda m: m["log_time"]),
                        reference[1] + reference[1], reference[2] + reference[2])
        else:
            expected = (reference[0][:48], [], [])
            assert f"recovery_stop={case['expected']['stop_offset']}" in stdout
        assert actual == expected, case["id"]
        assert inspect(HERE / case["output"]) == expected, "checked-in output: " + case["id"]
        counts = {"messages": len(actual[0]), "attachments": len(actual[1]), "metadata": len(actual[2])}
        assert all(counts[key] == case["expected"][key] for key in counts)
        row = {"case": case["id"], "result": "passed", **counts,
               "output": output.relative_to(ROOT).as_posix(),
               "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
        results.append(row)
        print(f"PASS {case['id']}: messages={counts['messages']} attachments={counts['attachments']} metadata={counts['metadata']}")
    (work / "report.json").write_text(json.dumps({"reference": manifest["reference"], "cases": results}, indent=2) + "\n",
                                      encoding="utf-8")
    print("All 3 runnable examples passed; report: _interop/examples/report.json")


if __name__ == "__main__":
    main()
