"""Require strict checks on supported targets and review every local warning exception."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def source_files():
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode("utf-8").split("\0")
    return sorted({name for name in names if name and (ROOT / name).is_file()})


def exception_scopes():
    scopes = []
    for name in source_files():
        if not name.endswith(".mbt"):
            continue
        text = (ROOT / name).read_text(encoding="utf-8")
        for attribute in re.finditer(r'#warnings\("([^"\n]+)"\)', text):
            assert re.fullmatch(r"-[a-z_]+", attribute[1]), "unreviewed compound warning exception: " + name
            end = text.find("\n///|", attribute.end())
            block = text[attribute.start():end if end >= 0 else len(text)]
            declaration = re.search(
                r"(?:pub(?:\(all\))?\s+)?(?:async\s+)?(?:fn|struct|enum|suberror|type)\s+([\w:]+)",
                text[attribute.end():],
            )
            assert declaration, "warning exception without declaration: " + name
            scopes.append({"file": name, "declaration": declaration[1], "warning": attribute[1][1:],
                           "scope_sha256": hashlib.sha256(block.encode("utf-8")).hexdigest()})
    return sorted(scopes, key=lambda row: (row["file"], row["declaration"], row["warning"]))


def audit_policy():
    manifest = json.loads((ROOT / "docs/static-exceptions.json").read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    actual = exception_scopes()
    expected = []
    for row in manifest["exceptions"]:
        assert set(row) == {"file", "declaration", "warning", "reason", "scope_sha256"}
        assert row["reason"].strip() and re.fullmatch(r"[0-9a-f]{64}", row["scope_sha256"])
        expected.append({k: v for k, v in row.items() if k != "reason"})
    assert actual == expected, "local warning scopes changed; review their code and update the explicit exception manifest"
    for name in source_files():
        path = ROOT / name
        if path.name in ("moon.mod", "moon.pkg"):
            text = path.read_text(encoding="utf-8")
            assert not re.search(r"(?:warnings|warn-list)\s*[:=]\s*[\"'][^\"']*-", text), "package/module-wide warning suppression: " + name
        if name.startswith(("tools/", "examples/")) and name.endswith(".py"):
            text = path.read_text(encoding="utf-8")
            assert not re.search(r"[\"']--warn-list[\"']\s*,\s*[\"']-a[\"']", text), "blanket suppression in verification script: " + name
    return manifest["exceptions"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=["wasm", "js", "all"], default="all")
    args = parser.parse_args()
    exceptions = audit_policy()
    moon = os.environ.get("MOON", "moon")
    identity = subprocess.check_output([moon, "version", "--all"], cwd=ROOT, text=True, encoding="utf-8")
    compiler = re.search(r"moonc v([^\s]+)", identity)
    assert compiler, "missing compiler identity"
    work = ROOT / "_interop/static"
    work.mkdir(parents=True, exist_ok=True)
    targets = ["wasm", "js"] if args.target == "all" else [args.target]
    rows = []
    for target in targets:
        start = time.monotonic()
        log = work / (target + ".jsonl")
        with log.open("wb") as output:
            result = subprocess.run([moon, "check", "--target", target, "--deny-warn", "--output-json", "--no-mbti"],
                                    cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, timeout=600)
        text = log.read_text(encoding="utf-8", errors="replace")
        diagnostics = []
        for line in text.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("$message_type") == "diagnostic":
                diagnostics.append(value)
        assert result.returncode == 0 and not diagnostics and not re.search(r"^Warning:", text, re.M), (
            f"strict {target} check failed; inspect _interop/static/{target}.jsonl"
        )
        rows.append({"target": target, "diagnostics": 0, "deny_warnings": True,
                     "seconds": round(time.monotonic() - start, 3)})
    report = {"result": "passed", "compiler": compiler[1], "checks": rows,
              "declaration_scoped_exceptions": len(exceptions), "exception_manifest": "docs/static-exceptions.json",
              "scope_hashes_match": True, "blanket_warning_suppression": False,
              "limits": ["Approved declaration exceptions are visible in the manifest; zero output does not imply no exceptions.",
                         "Host packages and file commands support JS only; Wasm checks cover the portable core."]}
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Strict static checks passed: {', '.join(targets)}; {len(exceptions)} reviewed declaration scopes")


if __name__ == "__main__":
    main()
