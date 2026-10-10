"""Count handwritten core implementation, excluding tests and generated interfaces."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
total = 0
for path in sorted(root.glob("*.mbt")):
    if path.name.endswith(("_test.mbt", "_wbtest.mbt")):
        continue
    count = sum(1 for line in path.read_text(encoding="utf-8-sig").splitlines()
                if line.strip() and not line.lstrip().startswith("//"))
    print(f"{path.name}: {count}")
    total += count
print(f"Effective handwritten core implementation lines: {total}")
if total < 2500:
    raise SystemExit("Implementation is below the requested 2500-line baseline")
