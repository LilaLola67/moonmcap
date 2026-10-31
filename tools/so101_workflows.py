"""Run the public SO-101 user example, then independently verify its artifacts."""
from html.parser import HTMLParser
import argparse
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import NonSeekingReader
from mcap.stream_reader import StreamReader


def messages(data):
    return [(c.topic, m.log_time, m.publish_time, m.sequence, m.data) for _, c, m in
            NonSeekingReader(io.BytesIO(data), validate_crcs=True).iter_messages(log_time_order=False)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-preview", action="store_true", help="explicitly regenerate the checked-in metadata-only preview")
    args = parser.parse_args()
    work = ROOT / "_interop/so101-example" / uuid.uuid4().hex
    work.mkdir(parents=True)
    output = work / "result with spaces 录制"
    with (work / "run.log").open("wb") as log:
        subprocess.run([sys.executable, "examples/so101/run.py", "--output", str(output)], cwd=ROOT,
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    source = ROOT / "_interop/public-corpus" / summary["source"]["name"]
    before = source.read_bytes()
    oracle = messages(before)
    choice = summary["selection"]
    expected = sorted([row for row in oracle if row[0] == choice["topic"] and int(choice["start"]) <= row[1] <= int(choice["end"])], key=lambda row: row[1])
    assert messages((output / "clip.mcap").read_bytes()) == expected
    prefix, failed = [], False
    try:
        for _, c, m in NonSeekingReader(io.BytesIO((output / "derived-truncated.mcap").read_bytes()), validate_crcs=True).iter_messages(log_time_order=False):
            prefix.append((c.topic, m.log_time, m.publish_time, m.sequence, m.data))
    except Exception:
        failed = True
    assert failed and len(prefix) == summary["recovered_prefix_messages"]
    assert messages((output / "recovered.mcap").read_bytes()) == prefix
    for name in ("clip.mcap", "recovered.mcap"):
        list(StreamReader(io.BytesIO((output / name).read_bytes()), validate_crcs=True).records)
    class Report(HTMLParser):
        def __init__(self):
            super().__init__(); self.links = []; self.rows = 0
        def handle_starttag(self, tag, attrs):
            if tag == "a": self.links += [value for key, value in attrs if key == "href"]
            if tag == "tr": self.rows += 1
    parsed = Report(); parsed.feed((output / "report.html").read_text(encoding="utf-8"))
    assert parsed.rows == len(expected) + 1
    for link in parsed.links:
        if not link.startswith("https://"):
            assert (output / link).is_file(), "broken result report link"
    second = subprocess.run([sys.executable, "examples/so101/run.py", "--output", str(output)], cwd=ROOT, capture_output=True)
    assert second.returncode != 0, "existing result directory overwritten"
    assert source.read_bytes() == before
    spec = importlib.util.spec_from_file_location("so101_demo", ROOT / "examples/so101/run.py")
    demo = importlib.util.module_from_spec(spec); spec.loader.exec_module(demo)
    preview = demo.render(summary, [{key: message[key] for key in ("sequence", "log_time", "publish_time")}
        for message in json.loads((output / "query.json").read_text(encoding="utf-8"))["messages"]])
    for name in ("clip.mcap", "source.midx", "recovered.mcap", "summary.json", "recovery.json"):
        preview = preview.replace(f'<a href="{name}">{name}</a>', f'<code>{name}</code>')
    preview = preview.replace('<div class="eyebrow">', '<p class="notice">固定验证结果的展示预览；执行复现命令后，可在本地报告中打开实际输出文件。</p><div class="eyebrow">', 1)
    preview_path = ROOT / "examples/so101/preview.html"
    if args.refresh_preview:
        preview_path.write_bytes(preview.encode("utf-8"))
    else:
        assert preview_path.read_text(encoding="utf-8") == preview, "preview differs; review and explicitly regenerate it"
    # A deterministic metadata-only preview; never copy raw recording or payloads.
    report = {"result": "passed", "source_sha256": summary["source"]["sha256"], "source_messages": len(oracle),
              "clip_messages": len(expected), "recovered_messages": len(prefix), "independent_reader": "mcap 1.3.0",
              "unicode_output_path": True, "existing_output_refused": True, "html_rows_and_links_checked": True,
              "original_unchanged": True, "physical_robot_connected": False}
    text = json.dumps(report, indent=2) + "\n"
    (work / "report.json").write_text(text, encoding="utf-8")
    (ROOT / "_interop/so101-example/report.json").write_text(text, encoding="utf-8")
    print("SO-101 example passed: full user workflow, independent payload/order/CRC oracle and report links")


if __name__ == "__main__":
    main()
