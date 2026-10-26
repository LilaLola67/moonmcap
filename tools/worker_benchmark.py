"""Same async pipeline, main-thread thresholds versus real codec workers; no speed assertions."""
import io,json,os,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/worker-benchmark'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/parallel/parallel.js';rows=[]
    for label,count,size in [('small',32,256),('larger',64,65536)]:
        for operation in ('decompress','decode'):
            data=io.BytesIO();writer=Writer(data,compression=CompressionType.ZSTD if operation=='decompress' else CompressionType.NONE,chunk_size=256*1024,enable_data_crcs=True);writer.start();cid=writer.register_channel('/bench','raw' if operation=='decompress' else 'json',0)
            for i in range(count):
                payload=(b'abcd'*(size//4)) if operation=='decompress' else json.dumps({'n':i,'blob':'x'*size},separators=(',',':')).encode()
                writer.add_message(cid,i,payload,i,i)
            writer.finish();source=work/f'{label}-{operation}.mcap';source.write_bytes(data.getvalue())
            for parallel in (False,True):
                args=[str(source),'--limit',str(count),'--workers','2' if parallel else '1','--min-chunk-bytes','0' if parallel else '2147483647','--min-payload-bytes','0' if parallel else '2147483647']
                if operation=='decode':args.append('--decode')
                start=time.perf_counter();result=subprocess.run([os.environ.get('NODE','node'),str(ROOT/'tools/rss_probe.cjs'),str(cli),*args],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True,encoding='utf-8',timeout=180)
                assert result.returncode==0,result.stderr
                seconds=round(time.perf_counter()-start,4);lines=result.stderr.splitlines();rss=json.loads(next(v.removeprefix('MOONMCAP_RSS=') for v in lines if v.startswith('MOONMCAP_RSS=')));metrics=json.loads(next(v for v in lines if v.startswith('{"pool"')))
                if parallel:assert metrics['pool']['submitted']>0
                else:assert metrics['pool']['submitted']==0
                rows.append({'shape':label,'operation':operation,'messages':count,'payload_bytes_each':size,'mode':'2 codec workers' if parallel else 'same pipeline main-thread thresholds','seconds':seconds,**rss,'pool':metrics['pool'],'readers':metrics['readers']})
                print(label,operation,rows[-1]['mode'],seconds,'s',rss['max_rss_kib'],'KiB',flush=True)
    report={'date':'2026-10-27','result':'measured','cases':rows,'notes':['Includes worker startup, metadata IO, codec work, hexadecimal transport and NDJSON formatting; stdout is discarded by the OS.','Main-thread case opens one idle worker but executes codec work on the main thread; this isolates the same command pipeline rather than comparing unrelated CLIs.','RSS includes all process threads and runtime; configured payload/transport budgets do not cap RSS.','Each shape/mode is measured once; no universal speedup or statistical performance claim.']}
    (ROOT/'_interop/worker-benchmark/report.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__':main()
