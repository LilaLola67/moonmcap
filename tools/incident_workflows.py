"""Independent MCAP writer/reader oracles for quality, events, context, capture and clocks."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader


def read(path):
    with path.open("rb") as stream:
        return [(c.topic, m.log_time, m.publish_time, m.sequence, m.data)
                for _, c, m in make_reader(stream, validate_crcs=True).iter_messages(log_time_order=False)]


def main():
    work = ROOT / "_interop/incidents" / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js", "--warn-list", "-a"],
                   cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / "_build/js/debug/build/cmd/incident/incident.js"
    checks = []
    serial = 0

    def run(mode, source, config, output=None, status=0):
        nonlocal serial
        serial += 1
        cfg = work / f"config-{serial}.json"
        cfg.write_text(json.dumps(config), encoding="utf-8")
        result = subprocess.run([os.environ.get("NODE", "node"), str(cli), mode, str(source), str(cfg),
                                 *([] if output is None else [str(output)])], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", timeout=120)
        assert result.returncode == status, (mode, result.returncode, result.stderr, result.stdout)
        return json.loads(result.stdout) if result.stdout.strip() else None

    rule = {"topic": "/sensor", "enter": {"path": "/v", "op": ">", "value": 20},
            "leave": {"path": "/v", "op": "<", "value": 10}, "duration": "10", "max_gap": "10",
            "pre": "10", "post": "10"}
    context = [{"topic": "/state", "depth": 2, "max_age": "100", "required": True}]
    for codec in CompressionType:
        data = io.BytesIO()
        writer = Writer(data, compression=codec, chunk_size=256, enable_data_crcs=True)
        writer.start()
        a = writer.register_channel("/sensor", "json", 0)
        b = writer.register_channel("/state", "json", 0)
        rows = []
        for n in range(21):
            t = n * 10
            value = 25 if 40 <= t <= 60 or 100 <= t <= 120 else 15 if t in (70, 130) else 0
            rows.append(("/sensor", t, t + 3, n, json.dumps({"v": value}).encode()))
        rows += [("/state", t, t + 3, n, json.dumps({"state": n}).encode()) for n, t in enumerate((0, 20))]
        # Physical disorder verifies source-time ordering and raw payload preservation.
        for topic, log, pub, seq, payload in rows[::2] + rows[1::2]:
            writer.add_message(a if topic == "/sensor" else b, log, payload, pub, seq)
        writer.finish()
        source = work / f"{codec.name}.mcap"
        source.write_bytes(data.getvalue())
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        quality = {"start": "0", "end": "200", "rules": [{"topic": "/sensor", "max_gap": "10",
                   "minimum_messages": 21, "check_sequence": True, "condition": {"path": "/v", "op": ">=", "value": 0}}],
                   "pairs": [{"left": "/sensor", "right": "/state", "tolerance": "0", "minimum_basis_points": 900}]}
        report = run("quality", source, quality)
        assert report["quality"]["status"] == "passed" and report["pairs"][0]["basis_points"] == 952
        quality["end"] = "220"
        report = run("quality", source, quality, status=1)
        assert report["quality"]["rules"][0]["details"] == [{"kind": "gap", "start": "200", "end": "220"}]
        quality["rules"] = [{"topic": "/missing", "warning": True}]
        quality.pop("pairs")
        assert run("quality", source, quality)["quality"]["status"] == "warning"
        config = {"rule": rule, "context": context, "topics": ["/sensor"]}
        prefix = work / (codec.name + "-event")
        report = run("events", source, config, prefix)
        expected_events = [{"start": "40", "confirmed": "50", "end": "80", "closed": True},
                           {"start": "100", "confirmed": "110", "end": "140", "closed": True}]
        assert report["events"] == expected_events
        # [30,90] and [90,150] merge, context retains both earlier state messages.
        assert len(report["windows"]) == 1
        expected = sorted([row for row in rows if row[0] == "/sensor" and 30 <= row[1] <= 150] +
                          [row for row in rows if row[0] == "/state"], key=lambda row: row[1])
        actual = read(Path(str(prefix) + "-0.mcap"))
        assert actual == expected, (actual, expected)
        run("events", source, config, prefix, status=2)  # Never overwrite a previous output.
        clip = work / (codec.name + "-clip.mcap")
        clipconfig = {"start": "40", "end": "60", "topics": ["/sensor"], "context": context}
        run("clip", source, clipconfig, clip)
        assert read(clip) == sorted([row for row in rows if row[0] == "/state" or
                                   row[0] == "/sensor" and 40 <= row[1] <= 60], key=lambda row: row[1])
        failure = work / (codec.name + "-missing.mcap")
        run("clip", source, {**clipconfig, "context": [{"topic": "/absent"}]}, failure, status=2)
        assert not failure.exists()
        run("clip", source, {**clipconfig, "clip_bytes": 32}, failure, status=2)
        assert not failure.exists()
        captureprefix = work / (codec.name + "-capture")
        report = run("capture", source, {"rule": rule, "buffer_messages": 4}, captureprefix)
        assert len(report["captures"]) == 2
        for i, (start, end) in enumerate(((40, 60), (100, 120))):
            assert read(Path(str(captureprefix) + f"-{i}.mcap")) == [row for row in rows if row[0] == "/sensor" and start <= row[1] <= end]
            assert report["captures"][i]["manifest"]["capture"]["post_complete"]
        clock = {"anchors": [{"source": "0", "target": "1000"}, {"source": "100", "target": "1101"},
                             {"source": "203", "target": "1307"}],
                 "probes": [{"source": "50", "target": "1050"}, {"source": "150", "target": "1201"}]}
        calibrated = work / (codec.name + "-clock.mcap")
        report = run("clock", source, clock, calibrated)
        assert all(row["residual_ns"] == "0" for row in report["residuals"]["probes"])
        expected = sorted(rows, key=lambda row: row[1])
        # Ties use stable source physical order, as in the independent reader.
        physical = rows[::2] + rows[1::2]
        expected = sorted(physical, key=lambda row: row[1])
        expected = [(topic, 1000 + t*101//100 if t <= 100 else 1101+(t-100)*206//103, pub, seq, payload)
                    for topic, t, pub, seq, payload in expected]
        assert read(calibrated) == expected
        failure = work / (codec.name + "-clock-fail.mcap")
        run("clock", source, {"anchors": [{"source": "10", "target": "0"}, {"source": "200", "target": "190"}]}, failure, status=2)
        assert not failure.exists()
        run("quality", source, {**quality, "unrecognized": 1}, status=2)
        assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
        checks.append({"compression": codec.name, "quality": "rules, boundary silence, warning, match availability",
                       "events": expected_events, "context": "raw payload and missing policy",
                       "capture": "incremental replay, two bounded windows", "clock": "independent integer piecewise oracle",
                       "atomic_failures": "existing output, missing context, clip budget, clock domain"})
    public = None
    if "--public" in sys.argv:
        source = ROOT / "_interop/public-corpus/so101-real.mcap"
        manifest = json.loads((ROOT / "examples/validation/public-corpus.json").read_text(encoding="utf-8"))
        origin = next(item for item in manifest["files"] if item["name"] == source.name)
        assert hashlib.sha256(source.read_bytes()).hexdigest() == origin["sha256"], "fetch the pinned public corpus first"
        physical = read(source)
        rows = sorted([row for row in physical if row[0] == "/joint_states"], key=lambda row: row[1])
        start, end = min(row[1] for row in physical), max(row[1] for row in physical)
        condition = {"path": "/name", "op": "exists"}
        quality = run("quality", source, {"start": str(start), "end": str(end), "decode": True,
                      "rules": [{"topic": "/joint_states", "minimum_messages": len(rows), "condition": condition}]})
        assert quality["quality"]["status"] == "passed"
        assert int(quality["quality"]["rules"][0]["messages"]) == len(rows)
        events = run("events", source, {"decode": True, "rule": {"topic": "/joint_states", "enter": condition,
                     "max_gap": "18446744073709551615"}})["events"]
        assert events == [{"start": str(rows[0][1]), "confirmed": str(rows[0][1]), "end": str(rows[-1][1]), "closed": False}]
        middle = rows[len(rows)//2][1]
        earlier = [row for row in physical if row[0] == "/tf_static" and row[1] < middle]
        assert earlier
        context = sorted(earlier, key=lambda row: row[1])[-1:]
        output = work / "public-context.mcap"
        run("clip", source, {"start": str(middle), "end": str(middle), "topics": ["/joint_states"],
            "context": [{"topic": "/tf_static", "depth": 1}]}, output)
        assert read(output) == context + [row for row in rows if row[1] == middle]
        public = {"source": origin["source"], "sha256": origin["sha256"], "kind": origin["kind"],
                  "total_messages": len(physical), "decoded_quality_messages": len(rows),
                  "checks": ["real ROS2 field existence and count", "decoded event boundaries", "raw context clip payloads"],
                  "clock_accuracy_evidence": False}
    report = {"result": "passed", "commands": serial, "codecs": checks,
              "data_origin": "synthetic independent Python MCAP writer; not real clock-calibration evidence", "public": public}
    name = "report-public.json" if public else "report.json"
    (ROOT / "_interop/incidents" / name).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Incident workflows passed: {serial} commands, three codecs, five workflows and failure cases")


if __name__ == "__main__":
    main()
