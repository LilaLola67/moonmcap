"""Build the exact Mooncakes archive and require every entry to be Git-tracked."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    run = subprocess.run([os.environ.get("MOON", "moon"), "package", "--list"], cwd=ROOT,
                         capture_output=True, text=True, encoding="utf-8")
    assert run.returncode == 0, "Moon package validation failed; inspect local package output"
    version = re.search(r'^version\s*=\s*"([^"]+)"', (ROOT / "moon.mod").read_text(encoding="utf-8"), re.M)[1]
    archive = ROOT / f"_build/publish/LilaLola67-moonmcap-{version}.zip"
    tracked = set(subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode("utf-8").split("\0"))
    with zipfile.ZipFile(archive) as package:
        entries = [i for i in package.infolist() if not i.is_dir()]
        assert entries and len({i.filename for i in entries}) == len(entries), "duplicate archive members"
        for info in entries:
            name = info.filename
            parts = PurePosixPath(name).parts
            assert not PurePosixPath(name).is_absolute() and ".." not in parts and "\\" not in name, "unsafe archive path"
            assert name in tracked, "archive includes an untracked file"
            assert not any(p.startswith(".") or p in ("_interop", "_interop_deps", "_build") for p in parts), "local-only directory in archive"
            assert not any(term in name for term in ("申报", "报名", "AGENTS.md", "credentials")), "private/local-only material in archive"
            if name.endswith((".md", ".mbt", ".mbti", ".py", ".cjs", ".mod", ".pkg")):
                content = package.read(info).decode("utf-8")
                assert not re.search(r"[A-Za-z0-9_.+-]+@(?:qq|163|126)\.com", content, re.I), "personal contact details in package"
        names = {i.filename for i in entries}
        assert {"moon.mod", "moon.pkg", "README.md", "LICENSE", "asyncio/moon.pkg", "third_party/README.md"} <= names
    work = ROOT / "_interop/package"
    work.mkdir(parents=True, exist_ok=True)
    report = {"version": version, "files": len(entries), "bytes": archive.stat().st_size,
              "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "tracked_files_only": True,
              "private_material_excluded": True, "registry_upload_performed": False}
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Package audit passed: {len(entries)} tracked files, {archive.stat().st_size} bytes, no registry upload")


if __name__ == "__main__":
    main()
