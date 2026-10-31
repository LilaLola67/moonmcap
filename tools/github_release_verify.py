"""Read-only check of a public GitHub Release, fixed tag, registry archive and assets."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MODULE = "LilaLola67/moonmcap"
API = "https://api.github.com/repos/" + MODULE


def fetch(url):
    # Public endpoints only. No publishing credential or authenticated redirect.
    request = urllib.request.Request(url, headers={"User-Agent": "MoonMCAP-release-audit"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"\d+\.\d+\.\d+", args.version)
    assert re.fullmatch(r"[0-9a-f]{40}", args.commit)
    tag = "v" + args.version
    release = json.loads(fetch(API + "/releases/tags/" + tag))
    assert not release["draft"] and not release["prerelease"] and release["published_at"]
    assert release["author"]["login"] == "LilaLola67"
    ref = json.loads(fetch(API + "/git/ref/tags/" + tag))["object"]
    assert ref["type"] == "tag", "expected an annotated release tag"
    tagged = json.loads(fetch(API + "/git/tags/" + ref["sha"]))
    assert tagged["object"]["type"] == "commit" and tagged["object"]["sha"] == args.commit
    assert args.commit in release["body"], "release notes omit fixed source commit"
    assets = {row["name"]: row for row in release["assets"]}
    archive_name = "LilaLola67-moonmcap-" + args.version + ".zip"
    wanted = {archive_name, "SHA256SUMS", "quality-evidence.json", "so101-preview.html"}
    assert set(assets) == wanted, "unexpected or missing release assets"
    bodies = {}
    for name, asset in assets.items():
        assert asset["state"] == "uploaded" and asset["uploader"]["login"] == "LilaLola67"
        expected_url = "https://github.com/" + MODULE + "/releases/download/" + tag + "/" + name
        assert asset["browser_download_url"] == expected_url
        bodies[name] = fetch(expected_url)
        assert len(bodies[name]) == asset["size"]
    checksums = {}
    for line in bodies["SHA256SUMS"].decode("utf-8").splitlines():
        digest, name = line.split("  ", 1)
        assert re.fullmatch(r"[0-9a-f]{64}", digest) and name not in checksums
        checksums[name] = digest
    assert set(checksums) == wanted - {"SHA256SUMS"}
    for name, digest in checksums.items():
        assert hashlib.sha256(bodies[name]).hexdigest() == digest, "asset checksum mismatch: " + name
    metadata = json.loads(fetch("https://mooncakes.io/api/v0/modules/" + MODULE + "@" + args.version))
    assert not metadata["yanked"] and metadata["build_status"] == "success"
    assert checksums[archive_name] == metadata["metadata"]["checksum"].lower(), "Release and registry archives differ"
    evidence = json.loads(bodies["quality-evidence.json"])
    assert evidence["commit"] == args.commit and evidence["version"] == args.version
    assert evidence["package_sha256"] == checksums[archive_name]
    preview = bodies["so101-preview.html"].decode("utf-8")
    assert 'href="clip.mcap"' not in preview and "固定验证结果的展示预览" in preview
    work = ROOT / "_interop/github-release"
    work.mkdir(parents=True, exist_ok=True)
    report = {"result": "passed", "version": args.version, "commit": args.commit,
              "release": release["html_url"], "tag": tag, "annotated_tag_matches": True,
              "registry_archive_matches": True, "asset_sha256": checksums,
              "limits": ["Checks current public assets; does not establish future immutability.",
                         "Detailed test evidence remains bound to the linked CI runs."]}
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("GitHub Release verified: annotated tag, fixed commit, four assets and exact registry archive")


if __name__ == "__main__":
    main()
