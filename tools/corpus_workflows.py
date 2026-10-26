"""Pinned public recording regression plus explicitly synthetic adversarial shapes."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import NonSeekingReader
from mcap.stream_reader import StreamReader
from mcap.writer import Writer, CompressionType


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public", action="store_true", help="also fetch and verify pinned publisher-provided recordings")
    args = parser.parse_args()
    work = ROOT / "_interop/corpus"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    diagnose = ROOT / "_build/js/debug/build/cmd/diagnose/diagnose.js"

    def command(entry, *values):
        result = subprocess.run([os.environ.get("NODE", "node"), str(entry), *map(str, values)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=180)
        assert result.returncode == 0, result.stderr + result.stdout[:3000]
        return json.loads(result.stdout)

    rows = []

    def verify(path, provenance):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        with path.open("rb") as stream:
            # This is a physical-order oracle, not the implementation's index.
            oracle = [(i, c.topic, m) for i, (_, c, m) in enumerate(NonSeekingReader(stream, validate_crcs=True).iter_messages(log_time_order=False))]
        with path.open("rb") as stream:
            list(StreamReader(stream, validate_crcs=True).records)
        report = command(diagnose, "validate", path, "--record-limit", "3000000")
        assert report["valid"] and int(report["observed_messages"]) == len(oracle)
        sidecar = work / (path.stem + ".midx")
        sidecar.unlink(missing_ok=True)
        command(cli, "index-build", path, "--output", sidecar)
        command(cli, "index-check", path, "--index", sidecar)
        topic = oracle[0][1]
        for descending, selected in ((False, None), (True, None), (False, topic)):
            wanted = sorted((row for row in oracle if selected is None or row[1] == selected), key=lambda row: (row[2].log_time, row[0]), reverse=descending)[3:16]
            common = ["--offset", 3, "--limit", 13]
            if descending:
                common.append("--descending")
            if selected:
                common += ["--topic", selected]
            for indexed in (False, True):
                options = ["index-query", path, "--index", sidecar] if indexed else ["query", path]
                result = command(cli, *options, *common)
                actual = [(m["topic"], int(m["log_time"]), int(m["publish_time"]), m["sequence"], bytes.fromhex(m["data_hex"])) for m in result["messages"]]
                expected = [(t, m.log_time, m.publish_time, m.sequence, m.data) for _, t, m in wanted]
                assert actual == expected
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        rows.append({"case": path.stem, **provenance, "sha256": digest, "bytes": path.stat().st_size,
                     "messages": len(oracle), "topics": sorted({t for _, t, _ in oracle}),
                     "checks": ["official physical-order oracle and CRC", "bounded diagnosis", "sidecar validation", "forward reverse topic paging through two providers", "input unchanged"],
                     "data_crc_verified": report["data_crc_verified"], "result": "passed"})

    for shape in ("same-time", "reverse", "overlap"):
        for mode in CompressionType:
            path = work / f"synthetic-{shape}-{mode.name}.mcap"
            with path.open("wb") as stream:
                writer = Writer(stream, compression=mode, chunk_size=16384, enable_crcs=True, enable_data_crcs=True)
                writer.start(library="official-python-shape-fixture")
                channels = [writer.register_channel(f"/sensor/{i}", "raw", 0) for i in range(32)]
                rng = random.Random(20261026)
                for i in range(384):
                    time = 7 if shape == "same-time" else (384 - i if shape == "reverse" else (i * 17) % 31)
                    if shape == "overlap" and i % 127 == 0:
                        time = 2**64 - 1
                    payload = rng.randbytes((0, 32, 4096, 65536)[i % 4])
                    writer.add_message(channels[i % len(channels)], time, payload, i, i)
                writer.finish()
            verify(path, {"kind": "synthetic", "shape": shape, "compression": mode.name})
    if args.public:
        manifest = json.loads((ROOT / "examples/validation/public-corpus.json").read_text(encoding="utf-8"))
        public = ROOT / "_interop/public-corpus"
        public.mkdir(parents=True, exist_ok=True)
        for item in manifest["files"]:
            path = public / item["name"]
            if not path.exists():
                # Fixed manifest names and URLs only; no caller-controlled paths.
                assert Path(item["name"]).name == item["name"]
                temporary = path.with_suffix(".download")
                try:
                    with urllib.request.urlopen(item["url"], timeout=60) as response, temporary.open("wb") as output:
                        size = 0
                        for data in iter(lambda: response.read(1024 * 1024), b""):
                            size += len(data)
                            assert size <= item["bytes"], "public corpus exceeds pinned size"
                            output.write(data)
                    assert size == item["bytes"]
                    assert hashlib.sha256(temporary.read_bytes()).hexdigest() == item["sha256"]
                    temporary.replace(path)
                finally:
                    temporary.unlink(missing_ok=True)
            assert path.stat().st_size == item["bytes"] and hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"], "public corpus fingerprint changed"
            verify(path, {"kind": item["kind"], "source": item["source"], "url": item["url"]})
    result = {"public_enabled": args.public, "note": "Synthetic shapes are labelled separately. Public recordings remain under their own terms and are not redistributed.", "cases": rows}
    (work / ("report-public.json" if args.public else "report.json")).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"Corpus workflows: {len(rows)} passed (public={args.public})")


if __name__ == "__main__":
    main()
