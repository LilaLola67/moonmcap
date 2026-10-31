"""Trace critical failure outcomes to tests, and optionally to a bound verification run."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def plan():
    manifest = json.loads((ROOT / "docs/risk-matrix.json").read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    rows = manifest["risks"]
    assert len({row["id"] for row in rows}) == len(rows)
    for row in rows:
        assert row["failure_outcome"] and row["tests"] and row["evidence"] and row["limits"]
        for name in row["tests"]:
            path = (ROOT / name).resolve()
            assert path.is_relative_to(ROOT) and path.is_file(), (row["id"], name)
    return rows


def evaluate(report):
    rows = []
    completed = [row["command"] for row in report["steps"] if row["exit_code"] == 0]
    for risk in plan():
        selected = report["profile"] in risk["profiles"]
        matches = [command for command in completed if any(command[:len(prefix)] == prefix for prefix in risk["evidence"])]
        if selected:
            assert matches, "required risk test did not run successfully: " + risk["id"]
        rows.append({"id": risk["id"], "selected": selected,
                     "test_execution_observed": bool(matches), "commands": matches,
                     "failure_outcome": risk["failure_outcome"], "limits": risk["limits"]})
    return {"measurement": "Execution traceability of named risk tests, not a coverage percentage or formal proof",
            "risks": rows, "source_unchanged": report["tracked_source_unchanged_during_run"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    risks = plan()
    report = {"result": "passed", "risks": len(risks), "traceability_only": True,
              "plan_sha256": hashlib.sha256((ROOT / "docs/risk-matrix.json").read_bytes()).hexdigest()}
    if args.evidence:
        evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
        assert evidence["result"] == "passed" and evidence["tracked_source_unchanged_during_run"]
        report.update({"traceability_only": False, "commit": evidence["commit"], "profile": evidence["profile"],
                       "execution": evaluate(evidence)})
    work = ROOT / "_interop/risks"
    work.mkdir(parents=True, exist_ok=True)
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Risk traceability passed:", len(risks), "outcomes;" , "execution bound" if args.evidence else "plan only")


if __name__ == "__main__":
    main()
