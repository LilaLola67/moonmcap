"""Real compiled MoonBit workers, independent order oracle and bounded failure paths."""
import hashlib,io,json,os,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType,IndexType
from mcap.reader import make_reader


def main():
    work=ROOT/'_interop/workers'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/parallel/parallel.js'
    def run(source,*args):return subprocess.run([os.environ.get('NODE','node'),str(cli),str(source),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=90)
    reports=[]
    for mode in CompressionType:
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=65536,enable_data_crcs=True);writer.start();channel=writer.register_channel('/json','json',0);expected=[]
        for i in range(96):
            payload=json.dumps({'n':9007199254740993+i,'blob':'abc'*2048}).encode();stamp=i*17%31;writer.add_message(channel,stamp,payload,stamp,i);expected.append((stamp,i,payload))
        writer.finish();source=work/f'{mode.name}.mcap';source.write_bytes(data.getvalue());digest=hashlib.sha256(data.getvalue()).hexdigest()
        for descending in (False,True):
            where=json.dumps({'path':'/n','op':'>=','value':9007199254741053})
            result=run(source,'--decode','--where',where,'--select','/n','--offset',3,'--limit',11,'--min-chunk-bytes',0,'--min-payload-bytes',0,*(['--descending'] if descending else []))
            assert result.returncode==0,result.stderr
            rows=[json.loads(line) for line in result.stdout.splitlines()];wanted=[v for v in sorted(expected,reverse=descending) if v[1]>=60][3:14]
            assert [(int(row['log_time']),row['sequence'],row['decoded']['data']['/n']) for row in rows]==[(t,i,9007199254740993+i) for t,i,_ in wanted]
            stats=json.loads(result.stderr);pool=stats['pool'];assert pool['active_peak']==2 and len(pool['threads'])==2 and pool['failed']==0,pool
            assert pool['peak_reserved_bytes']<=128*1024*1024 and all(r['peak_cursor_batch_bytes']<=128*1024*1024 for r in stats['readers'])
            reports.append({'compression':mode.name,'descending':descending,'result':'passed','pool':pool})
        small=run(source,'--decode','--min-chunk-bytes',1048576,'--min-payload-bytes',1048576,'--limit',3)
        assert small.returncode==0 and json.loads(small.stderr)['pool']['submitted']==0,small.stderr
        raw=run(source,'--min-chunk-bytes',0,'--limit',7);assert raw.returncode==0,raw.stderr
        rows=[json.loads(line) for line in raw.stdout.splitlines()];assert [bytes.fromhex(r['data_hex']) for r in rows]==[v[2] for v in sorted(expected)[:7]]
        assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
    source=work/'ZSTD.mcap';failures=[]
    for args,name in [(['--worker-buffer-bytes',1024],'buffer bound'),(['--worker-timeout',1],'startup deadline')]:
        output=work/(name.replace(' ','-')+'.ndjson');result=run(source,*args,'--output',output)
        assert result.returncode==2 and not output.exists(),(name,result.stderr)
        failures.append(name)
    for name,body in [('crash',"throw new Error('intentional worker crash')"),('malformed',"parentPort.postMessage({id:m.id,result:'zz'})"),('invalid-expanded',"parentPort.postMessage({id:m.id,result:'00'})")]:
        entry=work/f'{name}.cjs';entry.write_text("const {parentPort}=require('node:worker_threads');parentPort.on('message',m=>{"+body+"});parentPort.postMessage({ready:true});")
        output=work/f'{name}.ndjson';result=run(source,'--worker-entry',entry,'--min-chunk-bytes',0,'--output',output)
        assert result.returncode==2 and not output.exists(),(name,result.stderr)
        failures.append(name)
    # Error in an unused lookahead row must not invalidate an earlier limited query.
    data=io.BytesIO();writer=Writer(data,compression=CompressionType.ZSTD,chunk_size=1);writer.start();channel=writer.register_channel('/json','json',0);writer.add_message(channel,0,b'{"n":1}',0,0);writer.add_message(channel,1,b'invalid JSON',1,1);writer.finish();lazy=work/'lazy.mcap';lazy.write_bytes(data.getvalue())
    result=run(lazy,'--decode','--min-payload-bytes',0,'--min-chunk-bytes',0,'--limit',1);assert result.returncode==0 and len(result.stdout.splitlines())==1,result.stderr
    output=work/'lazy-fail.ndjson';result=run(lazy,'--decode','--min-payload-bytes',0,'--min-chunk-bytes',0,'--limit',2,'--output',output);assert result.returncode==2 and not output.exists()
    # Expanded bodies larger than max_record_bytes must still preserve the original nested record cap.
    data=io.BytesIO();writer=Writer(data,compression=CompressionType.ZSTD,chunk_size=20*1024*1024);writer.start();channel=writer.register_channel('/large','raw',0)
    for i in range(20):writer.add_message(channel,i,b'x'*(1024*1024),i,i)
    writer.finish();large=work/'expanded-large.mcap';large.write_bytes(data.getvalue());result=run(large,'--min-chunk-bytes',0,'--limit',1)
    assert result.returncode==0,result.stderr
    assert len(bytes.fromhex(json.loads(result.stdout)['data_hex']))==1024*1024
    data=io.BytesIO();writer=Writer(data,use_chunking=False,index_types=IndexType.NONE);writer.start();channel=writer.register_channel('/fallback','json',0);writer.add_message(channel,0,b'{"n":1}',0,0);writer.add_message(channel,1,b'{"n":2}',1,1);writer.finish();fallback=work/'unindexed.mcap';fallback.write_bytes(data.getvalue())
    result=run(fallback,'--decode','--min-payload-bytes',0,'--select','/n');assert result.returncode==0,result.stderr
    assert [json.loads(line)['decoded']['data']['/n'] for line in result.stdout.splitlines()]==[1,2]
    assert json.loads(result.stderr)['readers'][0]['indexed'] is False
    report={'result':'passed','cases':reports,'failure_cases':failures,'lazy_error_ordering':True,'expanded_body_over_16MiB':True,'unindexed_fallback':True,'source_unchanged':True,'note':'Actual worker thread IDs are reported; payload/cache reservations are distinct from total RSS.'}
    (ROOT/'_interop/workers/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Worker workflows passed: compiled MoonBit jobs on 2 threads, 3 codecs/order, small-block bypass, faults, lazy errors and >16MiB expansion')


if __name__=='__main__':main()
