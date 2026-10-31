"""Run named verification profiles and retain a commit-bound, hashed evidence bundle."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    # Windows consoles may default to a legacy encoding; reports and child logs
    # use UTF-8 so a failing check cannot prevent its own evidence being saved.
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
    os.environ["PYTHONUTF8"] = "1"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile", choices=["source", "full", "public", "release"], default="source"
    )
    parser.add_argument("--version")
    parser.add_argument("--commit")
    args = parser.parse_args()
    plan = json.loads(
        (ROOT / "tools/verification-plan.json").read_text(encoding="utf-8")
    )
    commands = list(plan[args.profile])
    if args.profile == "release":
        assert args.version and args.commit, "release requires --version and --commit"
        commands = [
            [
                part.replace("{version}", args.version).replace("{commit}", args.commit)
                for part in cmd
            ]
            for cmd in commands
        ]
    moon = os.environ.get("MOON", "moon")
    executables = {"python": sys.executable, "moon": moon}
    work = ROOT / "_interop/verification" / uuid.uuid4().hex
    work.mkdir(parents=True)

    def output(command):
        return subprocess.check_output(
            command, cwd=ROOT, text=True, encoding="utf-8", errors="replace"
        ).strip()

    def sanitize(text):
        # Shareable evidence excludes local user/workspace paths and credentials.
        for path, replacement in [
            (str(ROOT), "<workspace>"),
            (str(Path.home()), "<home>"),
        ]:
            text = text.replace(path, replacement).replace(
                path.replace("\\", "/"), replacement
            )
            text = text.replace(path.replace("\\", "\\\\"), replacement)
        return text

    def sanitize_value(value):
        if isinstance(value, str):
            return sanitize(value)
        if isinstance(value, list):
            return [sanitize_value(item) for item in value]
        if isinstance(value, dict):
            return {sanitize(key): sanitize_value(item) for key, item in value.items()}
        return value

    def source_fingerprint():
        digest = hashlib.sha256()
        paths = (
            subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
            .decode("utf-8")
            .split("\0")
        )
        for name in sorted(n for n in paths if n):
            path = ROOT / name
            digest.update(name.encode("utf-8") + b"\0")
            digest.update(
                hashlib.sha256(path.read_bytes()).digest()
                if path.is_file()
                else b"missing"
            )
        return digest.hexdigest()

    started = time.time()
    fingerprint = source_fingerprint()
    report = {
        "profile": args.profile,
        "result": "running",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "commit": output(["git", "rev-parse", "HEAD"]),
        "index_tree": output(["git", "write-tree"]),
        "working_tree_clean": not bool(output(["git", "status", "--porcelain"])),
        "source_version": re.search(
            r'^version = "([^"]+)"', (ROOT / "moon.mod").read_text(), re.M
        )[1],
        "runtime": {
            "moon": sanitize(output([moon, "version", "--all"])),
            "node": output(["node", "--version"]),
            "python": platform.python_version(),
            "platform": platform.system(),
            "machine": platform.machine(),
        },
        "steps": [],
        "planned_commands": commands,
        "evidence": [],
        "limits": [
            "Source profiles do not verify the registry upload.",
            "SDK and downloaded dependency caches can be reused.",
            "An unexecuted profile is not implied by another passing profile.",
        ],
    }
    failure = None
    for index, command in enumerate(commands):
        assert command and command[0] in executables
        argv = [executables[command[0]], *command[1:]]
        step_start = time.monotonic()
        log = work / f"{index + 1:02d}.log"
        print(f"[{index + 1}/{len(commands)}] " + " ".join(command), flush=True)
        with log.open("w", encoding="utf-8") as stream:
            try:
                result = subprocess.run(
                    argv,
                    cwd=ROOT,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    timeout=3600,
                )
                code = result.returncode
            except (OSError, subprocess.TimeoutExpired) as error:
                stream.write(sanitize(str(error)))
                code = -1
        content = sanitize(log.read_text(encoding="utf-8", errors="replace"))
        log.write_text(content, encoding="utf-8")
        report["steps"].append(
            {
                "command": command,
                "exit_code": code,
                "seconds": round(time.monotonic() - step_start, 3),
                "log": log.name,
                "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            }
        )
        if code:
            failure = " ".join(command)
            print(content[-4000:], flush=True)
            break
    report["result"] = "failed" if failure else "passed"
    report["tracked_worktree_sha256"] = fingerprint
    report["tracked_source_unchanged_during_run"] = source_fingerprint() == fingerprint
    if not report["tracked_source_unchanged_during_run"]:
        report["result"] = "failed"
        failure = failure or "tracked source changed during verification"
    report["unexecuted_commands"] = commands[len(report["steps"]) :]
    report["seconds"] = round(time.time() - started, 3)
    # Only reports created/updated during this run; never sweep stale successful reports.
    for path in sorted((ROOT / "_interop").glob("*/report*.json")):
        if path.parent.name == "verification" or path.stat().st_mtime < started:
            continue
        destination = work / (path.parent.name + "-" + path.name)
        destination.write_text(
            json.dumps(
                sanitize_value(json.loads(path.read_text(encoding="utf-8"))), indent=2
            )
            + "\n",
            encoding="utf-8",
        )
        report["evidence"].append(
            {
                "file": destination.name,
                "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                "bytes": destination.stat().st_size,
            }
        )
    text = json.dumps(report, indent=2) + "\n"
    (work / "manifest.json").write_text(text, encoding="utf-8")
    (ROOT / "_interop/verification/latest.json").write_text(text, encoding="utf-8")
    print(
        "Verification " + report["result"] + ": " + str(work.relative_to(ROOT)),
        flush=True,
    )
    if failure:
        raise SystemExit("Failed: " + failure)


if __name__ == "__main__":
    main()
