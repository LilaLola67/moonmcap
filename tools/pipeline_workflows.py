"""Unified query: independent order/filter oracle across backends and HTTP caches."""
from collections import defaultdict
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import io,json,os,re,subprocess,sys,threading,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/pipeline'/uuid.uuid4().hex;work.mkdir(parents=True)
    temporary=work/'temporary';temporary.mkdir();cache=work/'cache';cache.mkdir()
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    def run(tool,*args):
        return subprocess.run([os.environ.get('NODE','node'),str(ROOT/f'_build/js/debug/build/cmd/{tool}/{tool}.js'),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=120)
    paths=[];indexes=[];fields=[];expected=[];cases=0
    for number,mode in enumerate(CompressionType):
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=512,enable_data_crcs=True);writer.start();cid=writer.register_channel('/n','json',0)
        for n in range(256):
            t=(n*17)%43;writer.add_message(cid,t,json.dumps({'n':n,'blob':'x'*100}).encode(),t,n);expected.append((t,number,n))
        writer.finish();path=work/f'{mode.name}.mcap';path.write_bytes(data.getvalue());index=path.with_suffix('.midx2');field=path.with_suffix('.mfld')
        result=run('paged','build',path,index,temporary,'--page-entries',3);assert result.returncode==0,result.stderr
        result=run('field','build',path,field,'--field','/n');assert result.returncode==0,result.stderr
        paths.append(path);indexes.append(index);fields.append(field)
    predicate=json.dumps({'path':'/n','op':'>=','value':230})
    for sidecar in (False,True):
        for workers in (0,2):
            for descending in (False,True):
                result=run('query',*paths,*[x for p in fields for x in ('--field-index',p)],*([x for p in indexes for x in ('--index',p)] if sidecar else []),'--workers',workers,'--where',predicate,'--select','/n','--offset',7,'--limit',19,*(['--descending'] if descending else []))
                assert result.returncode==0,result.stderr
                rows=[json.loads(line) for line in result.stdout.splitlines()];wanted=[v for v in sorted(expected,reverse=descending) if v[2]>=230][7:26]
                assert [(int(r['log_time']),r['source_index'],r['sequence']) for r in rows]==wanted,(rows,wanted)
                stats=json.loads(result.stderr);assert all(p['content_verified'] and p['skipped_regions']>40 for p in stats['plans']),stats
                cases+=1
    # A tiny compressed span can expand past the worker threshold. Its job
    # reservation must use its actual declared size, not the global 64 MiB cap.
    data=io.BytesIO();writer=Writer(data,compression=CompressionType.ZSTD,chunk_size=1024*1024,enable_data_crcs=True);writer.start();cid=writer.register_channel('/large','json',0)
    for n in range(8):writer.add_message(cid,n,json.dumps({'n':n,'blob':'x'*30000}).encode(),n,n)
    writer.finish();large=work/'large.mcap';large.write_bytes(data.getvalue());large_index=work/'large.midx2';large_fields=work/'large.mfld'
    result=run('paged','build',large,large_index,temporary,'--page-entries',2);assert result.returncode==0,result.stderr
    result=run('field','build',large,large_fields,'--field','/n');assert result.returncode==0,result.stderr
    result=run('query',large,'--index',large_index,'--field-index',large_fields,'--workers',2,'--where','true','--select','/n');assert result.returncode==0,result.stderr
    assert [json.loads(line)['sequence'] for line in result.stdout.splitlines()]==list(range(8));assert json.loads(result.stderr)['pools'][0]['completed']>=9
    payloads={'/source':paths[2].read_bytes(),'/index':indexes[2].read_bytes(),'/fields':fields[2].read_bytes()};counts=defaultdict(int)
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*_):pass
        def handle(self):
            try:super().handle()
            except ConnectionError:pass
        def do_GET(self):
            counts[self.path]+=1;match=re.fullmatch(r'bytes=(\d+)-(\d+)',self.headers.get('Range',''))
            if self.path not in payloads or not match:self.send_error(404);return
            a,b=map(int,match.groups());data=payloads[self.path]
            self.send_response(206);self.send_header('Content-Range',f'bytes {a}-{b}/{len(data)}');self.send_header('ETag','"pipeline-v1"');self.send_header('Content-Length',b-a+1);self.end_headers()
            try:self.wfile.write(data[a:b+1])
            except ConnectionError:pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();base=f'http://127.0.0.1:{server.server_port}'
    try:
        for warm in (False,True):
            result=run('query',base+'/source','--index',base+'/index','--field-index',base+'/fields','--http-cache',cache,'--workers',2,'--where',predicate,'--select','/n','--limit',8)
            assert result.returncode==0,result.stderr
            rows=[json.loads(line) for line in result.stdout.splitlines()];wanted=[v for v in sorted(expected) if v[1]==2 and v[2]>=230][:8]
            assert [(int(r['log_time']),r['sequence']) for r in rows]==[(v[0],v[2]) for v in wanted]
            stats=json.loads(result.stderr)
            if warm:assert all(c['requests']==1 and c['disk_misses']==0 for c in stats['http_caches']),stats
            cases+=1
        bad=bytearray(fields[0].read_bytes());bad[-1]^=1;corrupt=work/'bad.mfld';corrupt.write_bytes(bad);output=work/'failed.ndjson'
        result=run('query',paths[0],'--field-index',corrupt,'--where','true','--output',output);assert result.returncode==2 and not output.exists()
        report={'result':'passed','queries':cases+1,'independent_messages':776,'codecs':3,'backends':['native','MIDX2'],'workers':[0,2],'remote_warm_cache':True,'high_ratio_MIDX2_worker_expansion':True,'corrupt_field_atomic_failure':True,'data_origin':'synthetic independent official Python MCAP writer'}
        (ROOT/'_interop/pipeline/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Unified pipeline passed: 11 queries, high-ratio MIDX2 worker expansion, global pagination, 3 codecs and warm HTTP caches')
    finally:server.shutdown();server.server_close();thread.join(timeout=2)


if __name__=='__main__':main()
