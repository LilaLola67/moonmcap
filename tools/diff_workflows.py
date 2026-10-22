"""Independent message/declaration/auxiliary diff and layout independence."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT/"_interop/diff"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT/"_build/js/debug/build/cmd/mcap/mcap.js"
    def run(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str,args)], cwd=ROOT, capture_output=True, text=True, timeout=60)
    def fixture(name, mode, change=None):
        b=io.BytesIO()
        w=Writer(b,compression=mode,chunk_size=256)
        w.start(library=name)
        channel=w.register_channel("/json", "json", 0, {"b":"2","a":"1"} if name=="a" else {"a":"1","b":"2"})
        for i in range(30 if change!="missing" else 29):
            data=b'{"a":1,"b":2}' if name=="a" else b'{"b":2.0,"a":1e0}'
            if change=="message" and i==12: data=b'{"a":9}'
            w.add_message(channel,i//3,data,i,i)
        w.add_attachment(1,2,"calibration","text/plain",b"changed" if change=="attachment" else b"same")
        w.add_metadata("config",{"version":"2" if change=="metadata" else "1"})
        w.finish()
        path=work/(name+".mcap")
        path.write_bytes(b.getvalue())
        return path
    a=fixture("a",CompressionType.LZ4)
    b=fixture("b",CompressionType.ZSTD)
    rows=[]
    for normalized in (False,True):
        result=run("diff",a,b,*(["--json-payloads"] if normalized else []))
        assert result.returncode==(0 if normalized else 1),result.stderr
        report=json.loads(result.stdout)
        assert report["equal"]==normalized and report["auxiliary_changes"]==0
        rows.append({"case":f"json-normalization-{normalized}","result":"passed"})
    for change in ("message","missing","attachment","metadata"):
        b=fixture(change,CompressionType.NONE,change)
        result=run("diff",a,b,"--json-payloads","--max-details",0)
        assert result.returncode==1,result.stderr
        report=json.loads(result.stdout)
        assert not report["equal"] and not report["details"] and report["omitted_details"]>0
        if change=="missing": assert report["left_only_messages"]==1 and report["right_only_messages"]==0
        if change=="message": assert report["left_only_messages"]==1 and report["right_only_messages"]==1
        if change in ("attachment","metadata"): assert report["auxiliary_changes"]==2
        rows.append({"case":change,"result":"passed"})
    damaged=work/"damaged.mcap"
    damaged.write_bytes(a.read_bytes()[:-3])
    assert run("diff",a,damaged).returncode==2
    (work/"report.json").write_text(json.dumps(rows,indent=2)+"\n")
    print(f"Semantic diff workflows: {len(rows)} passed")


if __name__=="__main__": main()
