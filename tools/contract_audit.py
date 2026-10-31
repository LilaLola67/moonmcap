"""Trace promises to existing implementation and tests; execution evidence is separate."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    data = json.loads((ROOT / "docs/contracts.json").read_text())
    rows = data["contracts"]
    assert data["version"] == 1 and len({r["id"] for r in rows}) == len(rows)
    for row in rows:
        assert set(row) == {"id", "promise", "conditions", "failure", "source", "tests"}
        assert (
            all(row[k] for k in ("promise", "conditions", "failure")) and row["tests"]
        )
        for name in [row["source"], *row["tests"]]:
            p = (ROOT / name).resolve()
            assert p.is_relative_to(ROOT) and p.is_file(), (row["id"], name)
    work = ROOT / "_interop/contracts"
    work.mkdir(parents=True, exist_ok=True)
    (work / "report.json").write_text(
        json.dumps(
            {
                "result": "passed",
                "contracts": len(rows),
                "traceability_only": True,
                "test_execution_verified_separately": True,
            },
            indent=2,
        )
        + "\n"
    )
    print(
        "Contract traceability passed:",
        len(rows),
        "promises; execution is verified separately",
    )


if __name__ == "__main__":
    main()
