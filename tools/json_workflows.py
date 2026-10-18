"""JSON predicates against Python Decimal and independently written MCAP files."""
import io
import json
import os
from decimal import Decimal
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import NonSeekingReader


def main():
    work = ROOT / "_interop/json"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"

    def run(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=90)

    expression = json.dumps({"all": [{"path": "/n", "op": ">", "value": 9007199254740992}, {"not": {"path": "/disabled", "op": "=", "value": True}}]})
    rows, paths, indexes, expected = [], [], [], []
    for source, compression in enumerate(CompressionType):
        buf = io.BytesIO()
        writer = Writer(buf, compression=compression, chunk_size=512, enable_crcs=True, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel("/sensor", "json", 0)
        raw = writer.register_channel("/raw", "raw", 0)
        for i in range(90):
            n = ["9007199254740992", "9007199254740993", "18446744073709551615", "9007199254740992.000000000000000001"][i % 4]
            payload = ('{"n":' + n + ',"disabled":' + str(i % 7 == 0).lower() + ',"nested":{"a/b":null}}').encode()
            writer.add_message(channel, 89-i, payload, 89-i, i)
            writer.add_message(raw, i, b"not JSON", i, i)
        writer.finish()
        path, index = work / f"{compression.name}.mcap", work / f"{compression.name}.midx"
        path.write_bytes(buf.getvalue())
        index.unlink(missing_ok=True)
        built = run("index-build", path, "--output", index)
        assert built.returncode == 0, built.stderr
        physical = [(c, m) for _, c, m in NonSeekingReader(io.BytesIO(buf.getvalue())).iter_messages(log_time_order=False)]
        matches = []
        for c, m in physical:
            if c.message_encoding != "json":
                continue
            doc = json.loads(m.data, parse_int=Decimal, parse_float=Decimal)
            if doc["n"] > Decimal(9007199254740992) and not doc["disabled"]:
                matches.append((source, m))
        expected.extend(matches)
        for descending in (False, True):
            wanted = sorted(matches, key=lambda p: (p[1].log_time, p[1].sequence), reverse=descending)[3:14]
            for command in ("query", "index-query"):
                args = [command, path, "--where", expression, "--select", "/n", "--select", "/nested/a~1b", "--select", "/missing", "--offset", 3, "--limit", 11]
                if command == "index-query":
                    args += ["--index", index]
                if descending:
                    args.append("--descending")
                result = run(*args)
                assert result.returncode == 0, result.stderr
                messages = json.loads(result.stdout, parse_float=Decimal)["messages"]
                assert [m["sequence"] for m in messages] == [m.sequence for _, m in wanted]
                assert all(m["decoded"]["missing_fields"] == ["/missing"] and m["decoded"]["data"]["/nested/a~1b"] is None for m in messages)
                assert [m["decoded"]["data"]["/n"] for m in messages] == [json.loads(m.data, parse_int=Decimal, parse_float=Decimal)["n"] for _, m in wanted]
                rows.append({"case": f"{compression.name}-{command}-{descending}", "result": "passed"})
        out = work / f"{compression.name}-filtered.mcap"
        out.unlink(missing_ok=True)
        result = run("stream-transform", path, "--temp-dir", work, "--output", out, "--where", expression, "--sample-period", 3, "--offset", 2, "--limit", 7)
        assert result.returncode == 0, result.stderr
        selected, bucket = [], None
        for _, m in sorted(matches, key=lambda p: p[1].log_time):
            if m.log_time // 3 != bucket:
                selected.append(m)
                bucket = m.log_time // 3
        actual = [m for _, _, m in NonSeekingReader(io.BytesIO(out.read_bytes())).iter_messages(log_time_order=False)]
        assert [(m.log_time, m.data) for m in actual] == [(m.log_time, m.data) for m in selected[2:9]]
        paths.append(path)
        indexes.append(index)
        rows.append({"case": compression.name+"-filter-before-sample", "result": "passed"})
    for sidecars in (False, True):
        args = ["dataset-query", *paths, "--where", expression, "--format", "ndjson", "--descending", "--offset", 5, "--limit", 19]
        if sidecars:
            for index in indexes:
                args += ["--index", index]
        result = run(*args)
        assert result.returncode == 0, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        wanted = sorted(expected, key=lambda p: (p[1].log_time, p[0], p[1].sequence), reverse=True)[5:24]
        assert [(m["source_index"], m["sequence"]) for m in actual] == [(i, m.sequence) for i, m in wanted]
        rows.append({"case": f"dataset-sidecars-{sidecars}", "result": "passed"})
    buf = io.BytesIO()
    writer = Writer(buf)
    writer.start()
    channel = writer.register_channel("/bad", "json", 0)
    writer.add_message(channel, 1, b'{"n":1,"n":2}', 1)
    writer.add_message(channel, 2, b'{"n":3}', 2)
    writer.finish()
    bad = work / "invalid.mcap"
    bad.write_bytes(buf.getvalue())
    output = work / "strict.ndjson"
    output.unlink(missing_ok=True)
    strict = run("query", bad, "--where", "true", "--format", "ndjson", "--output", output)
    assert strict.returncode != 0 and not output.exists()
    skipped = run("query", bad, "--where", "true", "--skip-invalid-json")
    assert skipped.returncode == 0 and len(json.loads(skipped.stdout)["messages"]) == 1
    assert not list(work.glob("*.part"))
    rows.append({"case": "strict-atomic-and-explicit-skip", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n")
    print(f"JSON content workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
