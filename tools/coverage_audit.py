"""Generate local coverage evidence and a risk-file inventory; no percentage quality gate."""

import json, os, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RISK = [
    "artifact_batch.mbt",
    "recording_session.mbt",
    "range_reader.mbt",
    "stream_writer.mbt",
    "sidecar.mbt",
    "cli_options.mbt",
]


def main():
    work = ROOT / "_interop/coverage"
    work.mkdir(parents=True, exist_ok=True)
    moon = os.environ.get("MOON", "moon")
    for name, args in [
        ("test", ["test", "--target", "js", "--enable-coverage", "--warn-list", "-a"]),
        (
            "report",
            ["coverage", "report", "-f", "coveralls", "-o", str(work / "raw.json")],
        ),
    ]:
        with (work / (name + ".log")).open("w", encoding="utf-8") as log:
            r = subprocess.run(
                [moon, *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT
            )
        if r.returncode:
            raise RuntimeError(
                "Coverage " + name + " failed; inspect " + str(work / (name + ".log"))
            )
    data = json.loads((work / "raw.json").read_text(encoding="utf-8"))
    rows = []
    for f in data["source_files"]:
        name = f["name"].replace("\\", "/")
        if name not in RISK:
            continue
        c = f["coverage"]
        rows.append(
            {
                "file": name,
                "covered_lines": sum(v is not None and v > 0 for v in c),
                "executable_lines": sum(v is not None for v in c),
                "uncovered_lines": [i + 1 for i, v in enumerate(c) if v == 0],
            }
        )
    assert {r["file"] for r in rows} == set(RISK)
    report = {
        "result": "passed",
        "measurement": "MoonBit JS unit-test line instrumentation only",
        "risk_files": rows,
        "selected_regressions": "risk_paths_test.mbt",
        "limitations": [
            "Not branch coverage.",
            "Does not measure embedded JavaScript host bodies.",
            "External CLI workflows are not merged into this trace.",
            "Uncovered lines remain candidates for review, not proof of defects.",
        ],
        "percentage_gate": False,
    }
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Coverage risk inventory generated:", len(rows), "files; no percentage gate")


if __name__ == "__main__":
    main()
