"""Persistent cache fault server: cold/warm processes, concurrency, corruption and crash."""
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/cached-http'/uuid.uuid4().hex;work.mkdir(parents=True)
    binary=bytes(range(256))*1024
    stream=io.BytesIO();writer=Writer(stream,compression=CompressionType.NONE,chunk_size=8192,enable_data_crcs=True);writer.start()
    channel=writer.register_channel('/cache','json',0)
    for i in range(64):writer.add_message(channel,i,json.dumps({'n':i,'blob':'x'*4096}).encode(),i,i)
    writer.finish();mcap=stream.getvalue()
    counts=defaultdict(int);active=defaultdict(int);peaks=defaultdict(int);lock=threading.Lock();version=[1];trace=[]
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*_):pass
        def handle(self):
            try:super().handle()
            except ConnectionError:pass
        def do_GET(self):
            mode=self.path.lstrip('/')
            with lock:counts[mode]+=1;call=counts[mode];active[mode]+=1;peaks[mode]=max(peaks[mode],active[mode])
            try:
                match=re.fullmatch(r'bytes=(\d+)-(\d+)',self.headers.get('Range',''))
                if not match:self.send_error(400);return
                start,end=map(int,match.groups());metadata=(start==end==0)
                if mode=='timeout' or (mode=='crash' and not metadata):time.sleep(.3)
                elif not metadata:time.sleep(.025)
                data=mcap if mode in ('mcap','changed') else binary
                if mode=='retry' and call==1:
                    self.send_response(503);self.send_header('Content-Length','0');self.end_headers();return
                if mode=='changed' and not metadata:
                    self.send_response(412);self.send_header('Content-Length','0');self.end_headers();return
                payload=data[start:end+1]
                status=200 if mode=='ignore' else 206
                self.send_response(status)
                total=len(data)+(1 if mode=='size-change' and not metadata else 0)
                self.send_header('Content-Range',f'bytes {start+(1 if mode=="bad-range" else 0)}-{end}/{total}')
                if mode!='noetag':self.send_header('ETag','W/"v1"' if mode=='weak' else ('"v2"' if mode=='etag-change' and not metadata else f'"v{version[0] if mode=="version" else 1}"'))
                if mode=='encoding':self.send_header('Content-Encoding','gzip')
                if mode=='oversize':payload+=b'overflow'
                if mode in ('oversize','short'):
                    self.send_header('Connection','close');self.close_connection=True
                else:self.send_header('Content-Length','999999' if mode=='bad-length' else str(len(payload)))
                self.end_headers()
                if mode=='short':payload=payload[:-1]
                try:self.wfile.write(payload)
                except ConnectionError:pass
                with lock:trace.append({'mode':mode,'start':start,'end':end,'if_match':self.headers.get('If-Match')})
            finally:
                with lock:active[mode]-=1
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/cached/cached.js';base=f'http://127.0.0.1:{server.server_port}'
    def command(mode,directory,*args):
        return [os.environ.get('NODE','node'),str(cli),base+'/'+mode,str(directory),'--page-bytes','4096','--disk-bytes','65536','--parallel','4','--prefetch','3',*map(str,args)]
    def run(mode,directory,*args):return subprocess.run(command(mode,directory,*args),cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=20)
    def stats(result):
        assert result.returncode==0,result.stderr
        return json.loads(result.stderr.splitlines()[-1])
    rows=[]
    try:
        cache=work/'warm';a=run('ok',cache,'--probe',0,8192,'--probe',0,8192);cold=stats(a)
        assert a.stdout.splitlines()==[binary[:8192].hex()]*2
        assert cold['requests']==6 and 1<cold['active_peak']<=4 and peaks['ok']<=4,cold
        before=counts['ok'];b=run('ok',cache,'--probe',0,8192);warm=stats(b)
        assert b.stdout.strip()==binary[:8192].hex() and warm['requests']==1 and counts['ok']-before==1 and warm['disk_hits']>0,warm
        rows.append({'case':'deduplicated parallel cold and warm independent process','cold':cold,'warm':warm})
        target=next(p for p in cache.glob('*.page') if p.stem.endswith('-0'))
        data=bytearray(target.read_bytes());data[-1]^=1;target.write_bytes(data)
        c=run('ok',cache,'--probe',0,8);repaired=stats(c)
        assert c.stdout.strip()==binary[:8].hex() and repaired['corrupt_pages']==1 and repaired['disk_misses']==1,repaired
        orphan=cache/('a'*64+'-'+'b'*24+'.part');orphan.write_bytes(b'crash residue')
        assert run('ok',cache,'--probe',0,8).returncode==0 and not orphan.exists()
        rows.append({'case':'checksum refetch and owned orphan cleanup','metrics':repaired})
        limited=run('eviction',work/'eviction','--disk-bytes',str(2*(4096+84)),'--probe',0,8)
        # Duplicate flags are rejected; use a command with the single intended budget.
        assert limited.returncode==2
        cmd=command('eviction',work/'eviction','--probe',0,8,'--probe',131072,8)
        cmd[cmd.index('--disk-bytes')+1]=str(2*(4096+84))
        limited=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=20);budget=stats(limited)
        assert budget['evictions']>=1 and budget['peak_disk_bytes']<=2*(4096+84),budget
        assert sum(p.stat().st_size for p in (work/'eviction').glob('*.page'))<=2*(4096+84)
        rows.append({'case':'bounded disk including staged page writes','metrics':budget})
        first=run('version',work/'version','--probe',0,8);stats(first);version[0]=2
        second=run('version',work/'version','--probe',0,8);assert stats(second)['disk_misses']>0
        rows.append({'case':'new ETag uses a new cache identity'})
        for mode in ('ignore','noetag','weak','bad-range','bad-length','oversize','short','encoding','size-change','etag-change','timeout'):
            result=run(mode,work/mode,'--retries','0','--timeout','30' if mode=='timeout' else '2000','--probe',0,8)
            assert result.returncode==2,(mode,result.stdout,result.stderr)
            assert not (work/mode/'.moonmcap-cache-lock').exists(),mode
            rows.append({'case':mode,'result':'rejected'})
        retry=stats(run('retry',work/'retry','--retries','1','--probe',0,8));assert retry['requests']==6,retry
        rows.append({'case':'bounded retry','metrics':retry})
        output=work/'changed.ndjson';result=run('changed',work/'changed','--output',output)
        assert result.returncode==2 and not output.exists() and not (work/'changed/.moonmcap-cache-lock').exists()
        rows.append({'case':'412 never publishes partial output'})
        result=run('mcap',work/'mcap','--output',work/'messages.ndjson','--limit',5)
        stats(result);messages=[json.loads(s) for s in (work/'messages.ndjson').read_text().splitlines()]
        assert len(messages)==5 and [int(m['log_time']) for m in messages]==list(range(5))
        rows.append({'case':'async MCAP query and atomic output'})
        unknown=work/'foreign';unknown.mkdir();(unknown/'personal.txt').write_text('preserve')
        result=run('ok',unknown,'--probe',0,8);assert result.returncode==2 and (unknown/'personal.txt').read_text()=='preserve'
        (cache/'foreign.txt').write_text('preserve')
        result=run('ok',cache,'--probe',0,8);assert result.returncode==2 and (cache/'foreign.txt').read_text()=='preserve' and not (cache/'.moonmcap-cache-lock').exists()
        rows.append({'case':'foreign files preserved and directory refused'})
        crash=work/'crash';child=subprocess.Popen(command('crash',crash,'--probe',0,8),cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+10
            while not (crash/'.moonmcap-cache-lock').exists() and child.poll() is None and time.monotonic()<deadline:time.sleep(.01)
            assert (crash/'.moonmcap-cache-lock').exists()
            competing=run('ok',crash,'--probe',0,8);assert competing.returncode==2 and (crash/'.moonmcap-cache-lock').exists()
        finally:child.kill();child.communicate(timeout=5)
        assert run('crash',crash,'--probe',0,8).returncode==0 and not (crash/'.moonmcap-cache-lock').exists()
        rows.append({'case':'live lease exclusion and killed process lease recovery'})
        report={'result':'passed','cases':rows,'server_peaks':dict(peaks),'cache_root':str(work.relative_to(ROOT))}
        (ROOT/'_interop/cached-http/report.json').write_text(json.dumps(report,indent=2)+'\n')
        print(f'Cached HTTP workflows passed: {len(rows)} cases, warm process reuses pages, disk limit and crash recovery')
    finally:server.shutdown();server.server_close();thread.join(timeout=2)


if __name__=='__main__':main()
