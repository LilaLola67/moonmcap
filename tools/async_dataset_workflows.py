"""Real HTTP + local async datasets, sidecars, JSON projection and failures."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"_interop_deps"))
from mcap.writer import Writer,CompressionType,IndexType


def main():
    work=ROOT/"_interop/async-dataset";work.mkdir(parents=True,exist_ok=True)
    subprocess.run([os.environ.get("MOON","moon"),"build","--target","js"],cwd=ROOT,check=True,capture_output=True)
    cli=ROOT/"_build/js/debug/build/cmd/dataset/dataset.js"
    mcap=ROOT/"_build/js/debug/build/cmd/mcap/mcap.js"
    payloads={};paths=[]
    for i,mode in enumerate((CompressionType.LZ4,CompressionType.ZSTD)):
        b=io.BytesIO();w=Writer(b,compression=mode,chunk_size=512,index_types=IndexType.NONE if i else IndexType.ALL)
        w.start();channel=w.register_channel("/json","json",0)
        for n in range(80):w.add_message(channel,(79-n)//2,json.dumps({"n":n}).encode(),n,n)
        w.finish();path=work/f"{i}.mcap";path.write_bytes(b.getvalue());paths.append(path);payloads[f"/{i}"]=b.getvalue()
    counts={};trace=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):pass
        def do_GET(self):
            counts[self.path]=counts.get(self.path,0)+1
            if self.path=="/changed" and counts[self.path]>1:
                self.send_response(412);self.send_header("Content-Length","0");self.end_headers();return
            data=payloads.get(self.path,payloads["/0"])
            match=re.fullmatch(r"bytes=(\d+)-(\d+)",self.headers.get("Range",""))
            if not match:self.send_error(400);return
            start,end=map(int,match.groups());body=data[start:end+1]
            self.send_response(206);self.send_header("Content-Range",f"bytes {start}-{end}/{len(data)}")
            self.send_header("Content-Length",str(len(body)));self.send_header("ETag",'"'+hashlib.sha256(data).hexdigest()+'"');self.end_headers()
            self.wfile.write(body);trace.append({"path":self.path,"range":[start,end],"if_match":self.headers.get("If-Match")})
    server=ThreadingHTTPServer(("127.0.0.1",0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    def run(binary,*args):return subprocess.run([os.environ.get("NODE","node"),str(binary),*map(str,args)],cwd=ROOT,capture_output=True,text=True,timeout=60)
    rows=[]
    try:
        url=f"http://127.0.0.1:{server.server_port}"
        indexes=[]
        for path in paths:
            index=path.with_suffix(".midx");index.unlink(missing_ok=True)
            built=run(mcap,"index-build",path,"--output",index);assert built.returncode==0,built.stderr;indexes.append(index)
        expected=[(source,n,(79-n)//2) for source in range(2) for n in range(70,80)]
        for sidecars in (False,True):
            for descending in (False,True):
                args=[url+"/0",paths[1],"--where",'{"path":"/n","op":">=","value":70}',"--select","/n","--offset",2,"--limit",5]
                if sidecars:
                    for index in indexes:args += ["--index",index]
                if descending:args += ["--descending"]
                result=run(cli,*args);assert result.returncode==0,result.stderr
                actual=[json.loads(line) for line in result.stdout.splitlines()]
                wanted=sorted(expected,key=lambda r:(r[2],r[0],r[1]),reverse=descending)[2:7]
                assert [(r["source_index"],r["sequence"],int(r["log_time"])) for r in actual]==wanted
                assert [r["decoded"]["data"]["/n"] for r in actual]==[n for _,n,_ in wanted]
                rows.append({"case":f"mixed-http-local-sidecars-{sidecars}-descending-{descending}","result":"passed"})
        output=work/"failed.ndjson";output.unlink(missing_ok=True)
        result=run(cli,url+"/changed",paths[1],"--output",output)
        assert result.returncode==2 and not output.exists()
        rows.append({"case":"snapshot-change-atomic-failure","result":"passed"})
        assert trace and all(item["if_match"] for item in trace if item["range"]!=[0,0])
    finally:server.shutdown();server.server_close();thread.join(timeout=2)
    (work/"report.json").write_text(json.dumps({"cases":rows,"range_requests":len(trace)},indent=2)+"\n")
    print(f"Unified async dataset workflows: {len(rows)} passed")


if __name__=="__main__":main()
