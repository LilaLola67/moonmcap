"""Compare two MoonMCAP diagnose builds on an identical CRC-protected fixture."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from large_source_workflows import ROOT, sha256, write_scale_fixture

LARGE_SHA256 = "ccf2c2c4b8b7c695fd896d9a3ca33bf84132dbbfc88e91a68582f0379170ae39"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="compiled baseline diagnose.js")
    parser.add_argument("candidate", type=Path, help="compiled candidate diagnose.js")
    parser.add_argument("--large", action="store_true", help="use the real 2 GiB fixture")
    parser.add_argument("--fixture", type=Path, help="reuse a file matching the pinned large fixture")
    parser.add_argument("--profile", action="store_true", help="record separate CPU profiles after timing")
    args = parser.parse_args()
    builds = {name: getattr(args, name).resolve(strict=True) for name in ("baseline", "candidate")}
    parent = ROOT / "_interop/scan-comparison"
    parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(dir=parent))
    print(work.relative_to(ROOT), flush=True)
    large = args.large or args.fixture is not None
    count = 2049 if large else 33
    source = args.fixture.resolve(strict=True) if args.fixture else work / "scan.mcap"
    if not args.fixture and large and shutil.disk_usage(work).free < 3 * 1024**3:
        raise SystemExit("Large comparison needs 3 GiB free disk space")
    node = os.environ.get("NODE", "node")
    expected = None

    def run(name, profile=False):
        nonlocal expected
        flags = []
        if profile:
            flags = ["--cpu-prof", "--cpu-prof-dir", str(work), "--cpu-prof-name", name + ".cpuprofile"]
        command = [node, *flags, str(ROOT / "tools/rss_probe.cjs"), str(builds[name]),
                   "validate", str(source), "--record-limit", "3000000"]
        started = time.perf_counter()
        process = subprocess.run(command, cwd=ROOT, check=True, capture_output=True,
                                 encoding="utf-8", timeout=1800)
        elapsed = time.perf_counter() - started
        result = json.loads(process.stdout)
        assert result["valid"] and result["data_valid"] and result["data_crc_verified"]
        assert int(result["observed_messages"]) == count and result["observed_time_start"] == "0"
        assert int(result["observed_time_end"]) == (count - 1) * 10
        if expected is None:
            expected = result
        assert result == expected, f"diagnostic output drift in {name}"
        rss = next(line for line in process.stderr.splitlines() if line.startswith("MOONMCAP_RSS="))
        usage = json.loads(rss.removeprefix("MOONMCAP_RSS="))
        print(f"{name} {'profile' if profile else 'timed'}: {elapsed:.3f}s", flush=True)
        return {"seconds": round(elapsed, 4), **usage}

    try:
        if not args.fixture:
            with source.open("xb") as stream:
                write_scale_fixture(stream, count, 1024 * 1024, 4 * 1024 * 1024)
        size = source.stat().st_size
        digest = sha256(source)
        if large:
            assert size > 2**31 and digest == LARGE_SHA256
        orders = [("baseline", "candidate"), ("candidate", "baseline")]
        samples = [{name: run(name) for name in order} for order in orders]
        if args.profile:
            for name in builds:
                run(name, profile=True)
        assert sha256(source) == digest
        report = {"result": "passed", "source_sha256": digest, "source_bytes": size,
                  "build_sha256": {name: sha256(path) for name, path in builds.items()},
                  "samples": samples, "order": orders, "diagnosis": expected,
                  "cpu_profiles": args.profile, "os_cache_state_controlled": False}
        (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    finally:
        if not args.fixture:
            source.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
