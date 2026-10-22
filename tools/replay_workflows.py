"""Real monotonic-clock replay against independent MCAP input and deadlines."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"_interop_deps"))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/"_interop/replay"
    work.mkdir(parents=True,exist_ok=True)
    subprocess.run([os.environ.get("MOON","moon"),"build","--target","js"],cwd=ROOT,check=True,capture_output=True)
    cli=ROOT/"_build/js/debug/build/cmd/replay/replay.js"
    rows=[]
    for mode in CompressionType:
        b=io.BytesIO();w=Writer(b,compression=mode,chunk_size=80);w.start()
        channel=w.register_channel("/json","json",0)
        times=[1000000000,1020000000,1040000000,1040000000]
        for i,t in enumerate(times):w.add_message(channel,t,json.dumps({"i":i}).encode(),t,i)
        w.finish();path=work/(mode.name+".mcap");path.write_bytes(b.getvalue())
        for rate in ("1/1","2/1"):
            result=subprocess.run([os.environ.get("NODE","node"),str(cli),str(path),"--rate",rate,"--where","true","--select","/i"],cwd=ROOT,capture_output=True,text=True,timeout=20)
            assert result.returncode==0,result.stderr
            records=[json.loads(line) for line in result.stdout.splitlines()]
            assert [r["sequence"] for r in records]==list(range(4))
            assert [r["decoded"]["data"]["/i"] for r in records]==list(range(4))
            clocks=[int(r["dispatch_clock_ns"]) for r in records]
            factor=int(rate.split("/")[0])
            for clock,t in zip(clocks,times):assert clock-clocks[0]>=(t-times[0])//factor-5000000
            assert clocks==sorted(clocks)
            rows.append({"case":mode.name+"-"+rate,"result":"passed","elapsed_ns":clocks[-1]-clocks[0]})
    result=subprocess.run([os.environ.get("NODE","node"),str(cli),str(path),"--rate","0/1"],cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==2
    (work/"report.json").write_text(json.dumps(rows,indent=2)+"\n")
    print(f"Replay workflows: {len(rows)} passed")


if __name__=="__main__":main()
