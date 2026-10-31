"""Retain the callable/type surface frozen from the published 0.17.3 interfaces."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def compact(text):
    # Int/UInt remain aliases of their explicit 32-bit spellings.
    text = re.sub(r"\bInt\b", "Int32", text)
    text = re.sub(r"\bUInt\b", "UInt32", text)
    return re.sub(r"\s+", "", text)


def declarations(text):
    lines = text.splitlines()
    result, attributes = set(), []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        index += 1
        if not line or line.startswith("//"):
            continue
        if line.startswith("#"):
            attributes.append(line)
            continue
        function = re.match(r"pub (?:async )?fn(?:\[[^\]]+\])?\s+([\w:]+)", line)
        if function:
            canonical = line
            for attribute in attributes:
                label = re.match(r"#label_migration\((\w+), alias=(\w+)\)", attribute)
                if label:
                    canonical = re.sub(r"\b" + label[1] + r"\?", label[2] + "?", canonical)
            result.add(compact(canonical))
            for attribute in attributes:
                alias = re.match(r"#alias\((\w+)\)", attribute)
                if alias:
                    name = function[1]
                    prefix = name.rsplit("::", 1)[0] + "::" if "::" in name else ""
                    result.add(compact(canonical.replace(name + "(", prefix + alias[1] + "(", 1)))
            attributes = []
            continue
        if re.match(r"(?:pub(?:\(all\))? )?(?:struct|enum|suberror|type|trait)\s+", line):
            block = [line]
            depth = line.count("{") - line.count("}")
            while depth:
                assert index < len(lines), "unclosed interface declaration"
                part = lines[index].strip()
                index += 1
                block.append(part)
                depth += part.count("{") - part.count("}")
            result.add(compact("\n".join(block)))
        elif line.startswith(("pub impl ", "pub let ", "pub const ")):
            result.add(compact(line))
        attributes = []
    return result


def main():
    baseline = json.loads((ROOT / "tests/fixtures/public-api-0.17.3.json").read_text(encoding="utf-8"))
    assert baseline["module"] == "LilaLola67/moonmcap" and baseline["version"] == "0.17.3"
    rows = []
    for name, item in baseline["interfaces"].items():
        current = declarations((ROOT / name).read_text(encoding="utf-8"))
        wanted = set(item["declarations"])
        missing = sorted(wanted - current)
        assert not missing, f"published interface changed in {name}: {missing[:8]}"
        rows.append({"interface": name, "retained_declarations": len(wanted), "additions": len(current - wanted),
                     "baseline_interface_sha256": item["sha256"]})
    work = ROOT / "_interop/api-audit"
    work.mkdir(parents=True, exist_ok=True)
    report = {"result": "passed", "baseline": baseline["source_commit"], "published_version": "0.17.3",
              "registry_sha256": baseline["registry_sha256"], "interfaces": rows,
              "limits": ["This is a source-signature compatibility check, not a behavioral equivalence proof.",
                         "JS host packages are now explicitly JS-only; unsupported Wasm host placeholders are not a supported API.",
                         "Behavior and on-disk formats are checked by separate regressions."]}
    (work / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Published API surface retained:", sum(row["retained_declarations"] for row in rows), "declarations")


if __name__ == "__main__":
    main()
