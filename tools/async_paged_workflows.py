"""Async MIDX2 oracle through local files and two cached remote snapshots."""
from collections import defaultdict
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import io,json,os,re,subprocess,sys,threading,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/async-paged'/uuid.uuid4().hex;work.mkdir(parents=True)
    temporary=work/'temporary';temporary.mkdir();cache=work/'cache';cache.mkdir()
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/asyncpaged/asyncpaged.js';paged=ROOT/'_build/js/debug/build/cmd/paged/paged.js'
    def command(entry,*args):return subprocess.run([os.environ.get('NODE','node'),str(entry),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=90)
    paths=[];indexes=[];expected=[]
    for file,mode in enumerate(CompressionType):
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=1024,enable_data_crcs=True);writer.start();channel=writer.register_channel('/numbers','json',0)
        for i in range(256):
            time=(i*17)%43;writer.add_message(channel,time,json.dumps({'n':i,'file':file}).encode(),time,i);expected.append((time,file,i))
        writer.finish();source=work/f'{mode.name}.mcap';source.write_bytes(data.getvalue());index=work/f'{mode.name}.midx2'
        result=command(paged,'build',source,index,temporary,'--page-entries',3);assert result.returncode==0,result.stderr
        paths.append(source);indexes.append(index)
    where=json.dumps({'path':'/n','op':'>=','value':150})
    for descending in (False,True):
        result=command(cli,*paths,*[x for index in indexes for x in ('--index',index)],'--where',where,'--select','/n','--decode','--offset',7,'--limit',19,*(['--descending'] if descending else []))
        assert result.returncode==0,result.stderr
        rows=[json.loads(line) for line in result.stdout.splitlines()]
        wanted=[v for v in sorted(expected,reverse=descending) if v[2]>=150][7:26]
        assert [(int(r['log_time']),r['source_index'],r['sequence']) for r in rows]==wanted,(rows,wanted)
    payloads={'/source':paths[2].read_bytes(),'/index':indexes[2].read_bytes()}
    bad=bytearray(payloads['/index']);bad[-1]^=1;payloads['/corrupt']=bytes(bad)
    counts=defaultdict(int);ranges=[]
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*_):pass
        def handle(self):
            try:super().handle()
            except ConnectionError:pass
        def do_GET(self):
            counts[self.path]+=1
            match=re.fullmatch(r'bytes=(\d+)-(\d+)',self.headers.get('Range',''))
            if self.path not in payloads or not match:self.send_error(404);return
            a,b=map(int,match.groups());data=payloads[self.path]
            self.send_response(206);self.send_header('Content-Range',f'bytes {a}-{b}/{len(data)}');self.send_header('ETag','"v1"');self.send_header('Content-Length',b-a+1);self.end_headers()
            try:self.wfile.write(data[a:b+1])
            except ConnectionError:pass
            ranges.append({'path':self.path,'start':a,'end':b,'if_match':self.headers.get('If-Match')})
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();base=f'http://127.0.0.1:{server.server_port}'
    try:
        remote=[]
        for warm in (False,True):
            result=command(cli,base+'/source','--index',base+'/index','--http-cache',cache,'--decode','--select','/n','--offset',3,'--limit',12)
            assert result.returncode==0,result.stderr
            rows=[json.loads(line) for line in result.stdout.splitlines()]
            wanted=sorted([v for v in expected if v[1]==2])[3:15]
            assert [(int(r['log_time']),r['sequence']) for r in rows]==[(v[0],v[2]) for v in wanted]
            metrics=json.loads(result.stderr.splitlines()[-1]);assert metrics['indexes'][0]['content_verified'] is True
            assert metrics['indexes'][0]['page_reads']>80
            if warm:assert all(c['requests']==1 and c['disk_misses']==0 for c in metrics['http_caches']),metrics
            remote.append({'warm':warm,'metrics':metrics})
        output=work/'failed.ndjson';result=command(cli,base+'/source','--index',base+'/corrupt','--http-cache',cache,'--output',output)
        assert result.returncode==2 and not output.exists()
        stale=work/'stale.mcap';changed=bytearray(paths[2].read_bytes());changed[15]^=1;stale.write_bytes(changed)
        result=command(cli,stale,'--index',indexes[2],'--output',output)
        assert result.returncode==2 and not output.exists()
        assert all(row['if_match']=='"v1"' for row in ranges if not (row['start']==row['end']==0))
        report={'result':'passed','compressions':3,'messages':768,'order_tests':['ascending','descending'],'providers':['local file','HTTP cached source and index'],'remote':remote,'failure_cases':['page CRC','stale source SHA256'],'requests':dict(counts)}
        (ROOT/'_interop/async-paged/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Async paged workflows passed: 3 codecs, global predicates/paging, remote warm caches, source and index failures')
    finally:server.shutdown();server.server_close();thread.join(timeout=2)


if __name__=='__main__':main()
