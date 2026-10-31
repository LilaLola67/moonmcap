"""Consume hash-pinned artifacts produced by the published historical module."""

import hashlib, json, os, re, shutil, struct, subprocess, uuid, zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    frozen = ROOT / "tests/fixtures/v0.17.1"
    manifest = json.loads((frozen / "manifest.json").read_text())
    assert (
        manifest["producer_module"] == "LilaLola67/moonmcap@0.17.1"
        and not manifest["source_path_override"]
    )
    for item in manifest["files"]:
        p = frozen / item["path"]
        assert (
            p.stat().st_size == item["bytes"]
            and hashlib.sha256(p.read_bytes()).hexdigest() == item["sha256"]
        )
    work = ROOT / "_interop/compatibility" / uuid.uuid4().hex
    work.mkdir(parents=True)
    data = work / "data"
    shutil.copytree(frozen, data)
    raw = (data / "recording/manifest.bin").read_bytes()
    value = json.loads(raw[12:-4])
    value["version"] = 2
    body = json.dumps(value, separators=(",", ":")).encode()
    (data / "future-recording.bin").write_bytes(
        b"MRSC1\r\n\0"
        + struct.pack("<I", len(body))
        + body
        + struct.pack("<I", zlib.crc32(body))
    )
    raw = bytearray((data / "source.midx").read_bytes())
    raw[4] = ord("9")
    (data / "future-index.bin").write_bytes(raw)
    consumer = work / "consumer"
    consumer.mkdir()
    version = re.search(
        r'^version\s*=\s*"([^"]+)"', (ROOT / "moon.mod").read_text(), re.M
    )[1]
    (consumer / "moon.mod").write_text(
        f'name = "local/compatibility"\nversion = "0.0.1"\nimport {{ "LilaLola67/moonmcap@{version}", }}\n'
    )
    (consumer / "moon.pkg").write_text(
        'import { "LilaLola67/moonmcap" @mcap, "LilaLola67/moonmcap/nodeio", }\npkgtype(kind:"executable")\n'
    )
    (consumer / "main.mbt").write_text(
        (ROOT / "tools/compatibility_consumer.mbt.txt").read_text()
    )
    (work / "moon.work").write_text(
        "members = ["
        + json.dumps(ROOT.as_posix(), ensure_ascii=False)
        + ', "consumer"]\n',
        encoding="utf-8",
    )
    p = subprocess.run(
        [os.environ.get("MOON", "moon"), "run", ".", "--target", "js", "--", str(data)],
        cwd=consumer,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    (work / "consumer.log").write_text(p.stdout + p.stderr, encoding="utf-8")
    assert p.returncode == 0 and "COMPATIBILITY_OK" in p.stdout, p.stderr[-3000:]
    for item in manifest["files"]:
        assert (
            hashlib.sha256((frozen / item["path"]).read_bytes()).hexdigest()
            == item["sha256"]
        ), "frozen fixture was changed"
        if item["path"].endswith(".mcap"):
            assert (data / item["path"]).read_bytes() == (
                frozen / item["path"]
            ).read_bytes(), "committed segment changed"
    report = {
        "result": "passed",
        "producer": manifest["producer_module"],
        "historical_files": len(manifest["files"]),
        "formats": ["MCAP0", "MIDX1", "MIDX2", "MRSC1", "ABAT1", "MLIV1"],
        "checks": [
            "hash-pinned published producer",
            "query equivalence",
            "source identity rejection",
            "completed recording and artifacts resume",
            "completed artifact not re-executed",
            "future versions rejected",
            "frozen inputs untouched",
        ],
        "policy": "No promise for unknown future formats. Recipe code fingerprints deliberately prohibit changed-code resume.",
    }
    (ROOT / "_interop/compatibility/report.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print("Historical compatibility passed:", len(manifest["files"]), "frozen files")


if __name__ == "__main__":
    main()
