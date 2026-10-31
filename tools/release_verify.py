"""Verify a fixed registry release against its Git commit and successful source CI."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import urllib.parse
import urllib.request
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "LilaLola67/moonmcap"
API = "https://api.github.com/repos/" + MODULE


def fetch(url):
    headers = {"User-Agent": "MoonMCAP-release-verification"}
    if url.startswith(API) and os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = "Bearer " + os.environ["GITHUB_TOKEN"]
    with urllib.request.urlopen(
        urllib.request.Request(url, headers=headers), timeout=90
    ) as response:
        return response.read()


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode().strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"\d+\.\d+\.\d+", args.version)
    assert re.fullmatch(r"[0-9a-f]{40}", args.commit)
    assert git("rev-parse", "HEAD") == args.commit, (
        "checkout must match requested commit"
    )
    assert not git("status", "--porcelain"), "release checkout must be clean"
    module = git("show", args.commit + ":moon.mod")
    assert re.search(r'^version = "' + re.escape(args.version) + '"$', module, re.M)
    work = ROOT / "_interop/release" / uuid.uuid4().hex
    work.mkdir(parents=True)
    # Exact release endpoint, never infer an older release from latest_version.
    metadata = json.loads(
        fetch(f"https://mooncakes.io/api/v0/modules/{MODULE}@{args.version}")
    )
    assert metadata["module"] == MODULE and metadata["version"] == args.version
    assert not metadata["yanked"] and metadata["build_status"] == "success"
    checksum = metadata["metadata"]["checksum"].lower()
    assert re.fullmatch(r"[0-9a-f]{64}", checksum)
    url = (
        "https://download.mooncakes.io/user/"
        + urllib.parse.quote(MODULE + "/" + args.version, safe="")
        + ".zip"
    )
    archive = fetch(url)
    assert hashlib.sha256(archive).hexdigest() == checksum, (
        "registry archive checksum mismatch"
    )
    tree = {}
    for row in git("ls-tree", "-r", "--full-tree", args.commit).splitlines():
        attributes, name = row.split("\t", 1)
        mode, kind, digest = attributes.split()
        if kind == "blob":
            tree[name] = digest
    with zipfile.ZipFile(io.BytesIO(archive)) as package:
        names = [i.filename for i in package.infolist() if not i.is_dir()]
        assert names and len(names) == len(set(names))
        assert {"moon.mod", "moon.pkg", "LICENSE", "README.md"} <= set(names)
        for name in names:
            path = PurePosixPath(name)
            assert (
                not path.is_absolute() and ".." not in path.parts and "\\" not in name
            )
            assert name in tree, "uncommitted member: " + name
            data = package.read(name)
            digest = hashlib.sha1(
                b"blob " + str(len(data)).encode() + b"\0" + data
            ).hexdigest()
            assert digest == tree[name], "archive differs from commit: " + name
    runs = json.loads(
        fetch(API + "/actions/runs?head_sha=" + args.commit + "&per_page=100")
    )["workflow_runs"]
    candidates = [
        r for r in runs if r["name"] == "check-build-test" and r["event"] == "push"
    ]
    assert candidates, "no source CI for exact commit"
    run = max(candidates, key=lambda r: r["id"])
    assert run["head_sha"] == args.commit and run["conclusion"] == "success", (
        "source CI incomplete or failed"
    )
    jobs = json.loads(fetch(API + f"/actions/runs/{run['id']}/jobs?per_page=100"))[
        "jobs"
    ]
    assert jobs and all(j["conclusion"] in ("success", "skipped") for j in jobs)
    required = {
        "core (js)",
        "core (wasm)",
        "current-toolchain",
        "platform-ubuntu-latest-node-22.3.0",
        "platform-windows-latest-node-22.3.0",
        "platform-windows-latest-node-22",
    }
    passed = {j["name"] for j in jobs if j["conclusion"] == "success"}
    assert required <= passed, "missing successful required CI jobs: " + str(
        sorted(required - passed)
    )
    # Installation must resolve the registry dependency, not a local path override.
    with (work / "install.log").open("w", encoding="utf-8") as log:
        subprocess.run(
            [sys.executable, "tools/install_smoke.py", "--version", args.version],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=900,
        )
    installed = json.loads(
        (ROOT / "_interop/installed/report.json").read_text(encoding="utf-8")
    )
    assert (
        installed["version"] == args.version
        and installed["fresh_consumer"]
        and not installed["source_path_override"]
    )
    docs_url = "https://mooncakes.io/docs/" + MODULE
    docs = fetch(docs_url).decode("utf-8")
    assert args.version in docs, "hosted latest API docs have not caught up"
    report = {
        "result": "passed",
        "version": args.version,
        "commit": args.commit,
        "tree": git("rev-parse", args.commit + "^{tree}"),
        "module": MODULE,
        "package_sha256": checksum,
        "package_bytes": len(archive),
        "package_members": len(names),
        "all_archive_members_match_commit": True,
        "source_ci": run["html_url"],
        "jobs": [{"name": j["name"], "conclusion": j["conclusion"]} for j in jobs],
        "installed": installed,
        "hosted_latest_docs": {"url": docs_url, "version_present": True},
        "limits": [
            "SDK and registry download caches may be reused; consumer directory is new.",
            "Hosted documentation check targets the latest published version.",
            "This verifier is read-only and does not publish packages.",
        ],
    }
    output = json.dumps(report, indent=2) + "\n"
    (work / "report.json").write_text(output, encoding="utf-8")
    (ROOT / "_interop/release/report.json").write_text(output, encoding="utf-8")
    print(
        f"Release {args.version} verified: fixed package, exact Git tree, CI, fresh consumer and hosted docs"
    )


if __name__ == "__main__":
    main()
