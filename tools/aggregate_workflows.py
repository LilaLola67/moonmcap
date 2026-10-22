"""Exact streaming aggregate results compared with Python Decimal."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from decimal import Decimal, getcontext

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType


def main():
    getcontext().prec = 100
    work = ROOT / "_interop/aggregate"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    def run(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, timeout=60)
    paths, expected, rows = [], {}, []
    for source, mode in enumerate(CompressionType):
        buf = io.BytesIO()
        writer = Writer(buf, compression=mode, chunk_size=512)
        writer.start()
        channel = writer.register_channel("/sensor", "json", 0)
        for i in range(70):
            value = ["9007199254740993", "-9007199254740992", "0.125", "-1e3"][i % 4]
            payload = ('{"device":'+str(i % 3)+',"n":'+value+'}').encode()
            writer.add_message(channel, 69-i, payload, 69-i, i)
            key = ((69-i)//7*7, i % 3)
            expected.setdefault(key, []).append(Decimal(value))
        writer.finish()
        path = work / f"{mode.name}.mcap"
        path.write_bytes(buf.getvalue())
        paths.append(path)
    for indexed in (False, True):
        args = ["aggregate", *paths, "--value", "/n", "--group", "/device", "--window", 7]
        if indexed:
            for path in paths:
                index = path.with_suffix(".midx")
                index.unlink(missing_ok=True)
                built = run("index-build", path, "--output", index)
                assert built.returncode == 0, built.stderr
                args += ["--index", index]
        result = run(*args)
        assert result.returncode == 0, result.stderr
        groups = json.loads(result.stdout)["groups"]
        assert len(groups) == len(expected)
        for row in groups:
            key = (int(row["window_start"]), row["group"]["fields"][0]["value"])
            values = expected[key]
            assert row["numeric_count"] == len(values)
            assert Decimal(row["sum"]) == sum(values)
            assert Decimal(row["min"]) == min(values) and Decimal(row["max"]) == max(values)
            assert Decimal(row["avg"]["numerator"])/row["avg"]["denominator"] == sum(values)/len(values)
        rows.append({"case": f"three-compressions-sidecars-{indexed}", "result": "passed"})
    for option in ("--max-groups", "--group-bytes"):
        target = work / (option[2:]+".json")
        target.unlink(missing_ok=True)
        result = run("aggregate", *paths, "--value", "/n", "--group", "/device", option, 1, "--output", target)
        assert result.returncode != 0 and not target.exists()
        rows.append({"case": option, "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n")
    print(f"Aggregate workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
