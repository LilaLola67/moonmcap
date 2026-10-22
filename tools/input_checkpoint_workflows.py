"""Terminate during ingest, reuse committed inputs, reject source drift."""
import io
import json
import os
from pathlib import Path
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
    assert data[:8]==b"MJOB2\r\n\0" and zlib.crc32(data[:-4])==struct.unpack_from("<I",data,len(data)-4)[0]
    pos=8+65
    inputs=struct.unpack_from("<I",data,pos)[0];pos+=4
    for _ in range(inputs+1):n=struct.unpack_from("<I",data,pos)[0];pos+=4+n
    messages=struct.unpack_from("<Q",data,pos)[0];pos+=32
    runs=struct.unpack_from("<I",data,pos)[0];pos+=4
    for _ in range(runs):
        pos+=12;n=struct.unpack_from("<I",data,pos)[0];pos+=4+n
    completed=struct.unpack_from("<I",data,pos)[0]
    return completed,messages


def main():
    work=ROOT/"_interop/input-checkpoint";work.mkdir(parents=True,exist_ok=True)
    subprocess.run([os.environ.get("MOON","moon"),"build","--target","js"],cwd=ROOT,check=True,capture_output=True)
    cli=ROOT/"_build/js/debug/build/cmd/mcap/mcap.js"
    base=[os.environ.get("NODE","node"),str(cli)]
    def command(*args):return subprocess.run([*base,*map(str,args)],cwd=ROOT,capture_output=True,text=True,timeout=180)
    inputs=[];expected=[]
    for source,count in enumerate((100,3000,3000)):
        b=io.BytesIO();w=Writer(b,compression=CompressionType.ZSTD,chunk_size=4096);w.start()
        schema=w.register_schema("Sample","jsonschema",json.dumps({"source":source}).encode())
        channel=w.register_channel("/source","raw",schema)
        for i in range(count):
            payload=bytes([source,i%256])*128;t=(i*17)%251
            w.add_message(channel,t,payload,i,i);expected.append((t,source,i,payload))
        w.add_attachment(1,2,f"source-{source}","application/octet-stream",b"data")
        w.add_metadata("config",{"source":str(source)});w.finish()
        path=work/f"input-{source}.mcap";path.write_bytes(b.getvalue());inputs.append(path)
    suffix=str(time.time_ns());job=work/("job-"+suffix);out=work/("result-"+suffix+".mcap")
    args=["external-merge",*inputs,"--job",job,"--output",out,"--run-bytes",8192,"--fan-in",8]
    process=subprocess.Popen([*base,*map(str,args)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    journal=job/"checkpoint.bin";observed=None;deadline=time.monotonic()+90
    while time.monotonic()<deadline and process.poll() is None:
        try:
            observed=progress(journal.read_bytes())
            if observed[0]==1:process.kill();break
        except FileNotFoundError:pass
        time.sleep(0.002)
    stdout,stderr=process.communicate(timeout=20)
    assert observed==(1,100) and process.returncode!=0,(observed,stdout,stderr)
    assert not out.exists()
    inspected=command("job-inspect",job);assert inspected.returncode==0,inspected.stderr
    status=json.loads(inspected.stdout)["progress"]
    assert status["version"]==2 and status["completed_inputs"]==1 and status["stage"]=="ingest"
    altered=work/"changed.mcap";data=bytearray(inputs[0].read_bytes());data[40]^=1;altered.write_bytes(data)
    changed=list(args);changed[1]=altered
    failed=command(*changed,"--resume");assert failed.returncode==2 and "fingerprint/order changed" in failed.stderr,failed.stderr
    resumed=command(*args,"--resume");assert resumed.returncode==0,resumed.stderr
    reader=NonSeekingReader(io.BytesIO(out.read_bytes()))
    actual=[(m.log_time,m.data[0],m.sequence,m.data) for _,_,m in reader.iter_messages(log_time_order=False)]
    assert actual==sorted(expected,key=lambda row:row[:3])
    assert len(list(NonSeekingReader(io.BytesIO(out.read_bytes())).iter_attachments()))==3
    assert len(list(NonSeekingReader(io.BytesIO(out.read_bytes())).iter_metadata()))==3
    assert command("job-clean",job).returncode==0 and out.exists()
    rows=[{"case":"kill-during-ingest","completed_inputs":1,"result":"passed"},{"case":"source-drift","result":"passed"},{"case":"resume-with-ID-collisions-and-auxiliaries","messages":6100,"result":"passed"}]
    (work/"report.json").write_text(json.dumps(rows,indent=2)+"\n")
    print(f"Input checkpoint workflows: {len(rows)} passed")


if __name__=="__main__":main()
