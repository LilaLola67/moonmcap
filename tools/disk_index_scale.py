"""Opt-in million-entry disk index build/query with real process RSS measurements."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType, IndexType


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--million", action="store_true")
    args = parser.parse_args()
    count = 1_000_000 if args.million else 8192
    work = ROOT / "_interop/disk-index-scale"
    temporary = work / "temporary"; temporary.mkdir(parents=True, exist_ok=True)
    source, index = work / "source.mcap", work / "index.midx2"
    index.unlink(missing_ok=True)
    with source.open("wb") as stream:
        writer = Writer(stream, compression=CompressionType.NONE, chunk_size=256*1024, index_types=IndexType.NONE, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel("/scale", "raw", 0)
        for i in range(count):
            writer.add_message(channel, (i*17)%10007, i.to_bytes(4, "little"), i, i)
        writer.finish()
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/paged/paged.js"
    rows=[]
    for name, values in (("build", ["build", source, index, temporary, "--record-limit", 3000000, "--spool-bytes", 1073741824, "--page-entries", 1024]),
                         ("query", ["query", source, index, "--record-limit", 3000000, "--start", 1234, "--end", 1234, "--limit", 13, "--cache-pages", 2])):
        started=time.perf_counter()
        result=subprocess.run([os.environ.get("NODE","node"),str(ROOT/"tools/rss_probe.cjs"),str(cli),*map(str,values)],cwd=ROOT,capture_output=True,text=True,encoding="utf-8",timeout=1800)
        assert result.returncode==0,result.stderr+result.stdout
        usage=json.loads(next(line.removeprefix("MOONMCAP_RSS=") for line in result.stderr.splitlines() if line.startswith("MOONMCAP_RSS=")))
        report=json.loads(result.stdout)
        if name=="build":
            assert int(report["entries"])==count and not list(temporary.iterdir())
        else:
            wanted=[i for i in range(count) if (i*17)%10007==1234][:13]
            assert [m["sequence"] for m in report["messages"]]==wanted
            assert [bytes.fromhex(m["data_hex"]) for m in report["messages"]]==[i.to_bytes(4,"little") for i in wanted]
            assert report["index"]["peak_cached_pages"]<=2
        rows.append({"operation":name,"seconds":round(time.perf_counter()-started,4),**usage,"report":report})
        print(f"Disk index {name}: {count} entries, {usage['max_rss_kib']} KiB peak RSS",flush=True)
    report={"million":args.million,"entries":count,"source_bytes":source.stat().st_size,"index_bytes":index.stat().st_size,"note":"RSS includes runtime; page and sorting payload budgets are separate. Opening audits all index pages and hashes the source.","cases":rows}
    (work/("report-million.json" if args.million else "report.json")).write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print("Disk index scale passed")


if __name__=="__main__":main()
