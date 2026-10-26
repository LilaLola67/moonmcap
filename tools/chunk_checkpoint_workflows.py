"""Kill during an input Chunk; resume committed copies and reject damaged state."""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time
import zlib
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"_interop_deps"))
from mcap.writer import Writer,CompressionType
from mcap.reader import NonSeekingReader


def progress(data):
    assert data[:8]==b"CJOB1\r\n\0" and zlib.crc32(data[:-4])==struct.unpack_from("<I",data,len(data)-4)[0]
    position=8+69;inputs=struct.unpack_from("<I",data,position)[0];position+=4
    for _ in range(inputs):n=struct.unpack_from("<I",data,position)[0];position+=4+n
    return struct.unpack_from("<II",data,position)


def main():
    work=ROOT/"_interop/chunk-checkpoint";temporary=work/"temporary";temporary.mkdir(parents=True,exist_ok=True)
    subprocess.run([os.environ.get("MOON","moon"),"build","--target","js"],cwd=ROOT,check=True,capture_output=True)
    cli=ROOT/"_build/js/debug/build/cmd/chunkmerge/chunkmerge.js";management=ROOT/"_build/js/debug/build/cmd/mcap/mcap.js"
    def command(entry,*values):return subprocess.run([os.environ.get("NODE","node"),str(entry),*map(str,values)],cwd=ROOT,capture_output=True,text=True,encoding="utf-8",timeout=180)
    sources=[];expected=[]
    for source,mode in enumerate((CompressionType.ZSTD,CompressionType.LZ4)):
        b=io.BytesIO();w=Writer(b,compression=mode,chunk_size=1024,enable_crcs=True,enable_data_crcs=True);w.start();c=w.register_channel("/shared","raw",0)
        for i in range(160):
            t=(i*17)%37;payload=bytes([source,i])*64;w.add_message(c,t,payload,i,i);expected.append((t,source,i,payload))
        w.add_metadata("source",{"id":str(source)});w.add_attachment(1,2,f"source-{source}","application/octet-stream",b"attachment");w.finish()
        path=work/f"source-{source}.mcap";path.write_bytes(b.getvalue());sources.append(path)
    suffix=str(time.time_ns());job=work/("job-"+suffix);output=work/("result-"+suffix+".mcap")
    values=[*sources,"--job",job,"--output",output,"--compression","zstd","--run-bytes",4096,"--fan-in",4]
    # An isolated test preload pauses at a real filesystem boundary. The
    # production CLI and library contain no test hook or timing dependency.
    preload=work/"pause-boundary.cjs"
    preload.write_text("""const fs=require('node:fs'),path=require('node:path');
const flag=process.env.CHUNK_BOUNDARY_FLAG,phase=process.env.CHUNK_BOUNDARY_PHASE;
let commits=0;
const stop=()=>{fs.writeFileSync(flag,'ready',{flag:'wx'});Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,60000);};
const rename=fs.renameSync;fs.renameSync=function(a,b){const r=rename.apply(this,arguments);if(phase==='ingest'&&path.basename(String(b))==='checkpoint.bin'&&++commits===2)stop();return r;};
const open=fs.openSync;fs.openSync=function(p,mode){const r=open.apply(this,arguments);if(phase==='merge'&&mode==='wx'&&path.basename(String(p))==='10000.run')stop();return r;};
""",encoding="utf-8")
    def paused(phase,resume):
        flag=work/(phase+"-"+suffix+".ready")
        env={**os.environ,"CHUNK_BOUNDARY_FLAG":str(flag),"CHUNK_BOUNDARY_PHASE":phase}
        p=subprocess.Popen([os.environ.get("NODE","node"),"--require",str(preload),str(cli),*map(str,values),*(["--resume"] if resume else [])],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        deadline=time.monotonic()+90
        while time.monotonic()<deadline and p.poll() is None and not flag.exists():time.sleep(0.002)
        if not flag.exists():
            p.kill();stdout,stderr=p.communicate(timeout=20)
            raise AssertionError((phase,stdout,stderr))
        p.kill();p.communicate(timeout=20)
        return progress((job/"checkpoint.bin").read_bytes())
    observed=paused("ingest",False)
    assert observed==(0,2),observed
    assert not output.exists()
    status=command(management,"job-inspect",job);assert status.returncode==0,status.stderr
    assert json.loads(status.stdout)["progress"]["engine"]=="indexed-chunk-ingest"
    damaged=work/("damaged-"+suffix);shutil.copytree(job,damaged)
    part=damaged/"0.run";data=bytearray(part.read_bytes());data[-1]^=1;part.write_bytes(data)
    changed=list(values);changed[changed.index(job)]=damaged
    failure=command(cli,*changed,"--resume")
    assert failure.returncode==2 and "part fingerprint changed" in failure.stderr and not output.exists(),failure.stderr
    assert command(management,"job-clean",damaged).returncode==0
    altered=work/"changed-source.mcap";data=bytearray(sources[0].read_bytes());data[35]^=1;altered.write_bytes(data)
    changed=list(values);changed[0]=altered
    failure=command(cli,*changed,"--resume")
    assert failure.returncode==2 and "input fingerprint/order changed" in failure.stderr and not output.exists(),failure.stderr
    final=paused("merge",True)
    assert final==(2,0) and (job/"10000.run").exists() and not output.exists(),final
    (job/"999999.run").write_bytes(b"incomplete orphan")
    result=command(cli,*values,"--resume");assert result.returncode==0,result.stderr
    report=json.loads(result.stdout)
    with output.open("rb") as stream:
        rows=[(m.log_time,m.data[0],m.sequence,m.data) for _,_,m in NonSeekingReader(stream,validate_crcs=True).iter_messages(log_time_order=False)]
    assert rows==sorted(expected,key=lambda r:r[:3])
    with output.open("rb") as stream:assert len(list(NonSeekingReader(stream).iter_attachments()))==2
    with output.open("rb") as stream:assert len(list(NonSeekingReader(stream).iter_metadata()))==2
    assert report["source_complete"] and report["source_data_crc_verified"]==[True,True] and not list(temporary.iterdir())
    assert command(management,"job-clean",job).returncode==0 and output.exists()
    result={"cases":["kill-after-committed-chunks","corrupt-part-rejected","source-drift-rejected","kill-during-final-sort","orphan-pruned","stable-resume-with-auxiliaries"],"committed_chunk_at_kill":observed[1],"messages":320,"source_sha256":[hashlib.sha256(p.read_bytes()).hexdigest() for p in sources],"result":"passed"}
    (work/"report.json").write_text(json.dumps(result,indent=2)+"\n")
    print("Chunk checkpoint workflows: 6 passed")


if __name__=="__main__":main()
