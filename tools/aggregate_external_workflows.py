"""Exact group spills versus independent Decimal; atomic failure and paged inputs."""
from decimal import Decimal, localcontext, ROUND_HALF_UP
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
    work=ROOT/"_interop/aggregate-external";temporary=work/"temporary";temporary.mkdir(parents=True,exist_ok=True)
    subprocess.run([os.environ.get("MOON","moon"),"build","--target","js"],cwd=ROOT,check=True,capture_output=True)
    cli=ROOT/"_build/js/debug/build/cmd/spill/spill.js"
    paged=ROOT/"_build/js/debug/build/cmd/paged/paged.js"
    def command(entry,*args):return subprocess.run([os.environ.get("NODE","node"),str(entry),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding="utf-8",timeout=180)
    sources=[];expected={}
    with localcontext() as context:
        context.prec=200
        for file,mode in enumerate(CompressionType):
            data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=1024,enable_data_crcs=True);writer.start();channel=writer.register_channel("/numbers","json",0)
            for i in range(2048):
                group=i%1201;time=i%47;key=(group,time//10*10)
                value=("9007199254740993","-9007199254740992","0.00025","1e3")[(i+file)%4]
                payload=f'{{"g":{group},"n":{value}}}'.encode() if i%11 else f'{{"g":{group}}}'.encode()
                writer.add_message(channel,time,payload,time,i)
                row=expected.setdefault(key,{"count":0,"values":[]})
                row["count"]+=1
                if i%11:row["values"].append(Decimal(value))
            writer.finish();source=work/f"{mode.name}.mcap";source.write_bytes(data.getvalue());sources.append(source)
        for indexed in (False,True):
            indexes=[]
            if indexed:
                for source in sources:
                    index=source.with_suffix(".midx2");index.unlink(missing_ok=True)
                    result=command(paged,"build",source,index,temporary)
                    assert result.returncode==0,result.stderr
                    indexes += ["--index",index]
            output=work/f"groups-{indexed}.ndjson";output.unlink(missing_ok=True)
            base=[*sources,"--output",output,"--temp-dir",temporary,"--value","/n","--group","/g","--window",10,"--max-groups",7,"--run-bytes",16384,"--merge-bytes",65536,"--fan-in",8,*indexes]
            result=command(cli,*base);assert result.returncode==0,result.stderr
            report=json.loads(result.stdout);rows=[json.loads(line) for line in output.read_text().splitlines()]
            assert len(rows)==len(expected)>1024 and report["runs_created"]>1
            for row in rows:
                key=(row["group"]["fields"][0]["value"],int(row["window_start"]))
                wanted=expected[key];values=wanted["values"]
                assert row["count"]==wanted["count"] and row["numeric_count"]==len(values)
                assert row["missing_count"]==wanted["count"]-len(values)
                if values:
                    total=sum(values,Decimal(0))
                    assert Decimal(row["sum"])==total and Decimal(row["min"])==min(values) and Decimal(row["max"])==max(values)
                    assert Decimal(row["avg"]["numerator"])==total and row["avg"]["denominator"]==len(values)
                    assert Decimal(row["avg"]["decimal"])==(total/len(values)).quantize(Decimal("1e-18"),rounding=ROUND_HALF_UP)
                else:assert row["sum"] is None and row["avg"] is None
            assert report["peak_run_bytes"]<=16384 and report["peak_merge_bytes"]<=65536 and not list(temporary.iterdir())
        for flag,value in (("--spool-bytes",64),("--run-limit",1),("--merge-bytes",128),("--group-bytes",100)):
            output=work/"failed.ndjson";output.unlink(missing_ok=True)
            result=command(cli,*sources,"--output",output,"--temp-dir",temporary,"--value","/n","--group","/g","--max-groups",2,flag,value)
            assert result.returncode==2 and not output.exists() and not list(temporary.iterdir()),result.stderr
    result={"input_messages":6144,"groups":len(expected),"providers":["range","paged MIDX2"],"failure_cases":4,"result":"passed"}
    (work/"report.json").write_text(json.dumps(result,indent=2)+"\n")
    print(f"External aggregate workflows passed: {len(expected)} exact groups, 2 providers, 4 atomic failures")


if __name__=="__main__":main()
