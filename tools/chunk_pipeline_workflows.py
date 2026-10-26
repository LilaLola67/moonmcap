"""Terminate after a durable final-merge group, resume both checkpoint stages."""
import hashlib,io,json,os,shutil,struct,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType
from mcap.reader import NonSeekingReader
from checkpoint_workflows import committed_run_ids


def main():
    work=ROOT/'_interop/chunk-pipeline'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/chunkmerge/chunkmerge.js';maincli=ROOT/'_build/js/debug/build/cmd/mcap/mcap.js'
    sources=[];expected=[]
    for file,mode in enumerate((CompressionType.ZSTD,CompressionType.LZ4)):
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=2048,enable_data_crcs=True);writer.start(profile='pipeline')
        channel=writer.register_channel('/shared','raw',0,metadata={'source':str(file)})
        for i in range(160):
            payload=bytes([file,i])*64;stamp=(159-i)%31;writer.add_message(channel,stamp,payload,i,i);expected.append((stamp,file,i,payload))
        writer.add_attachment(1,2,f'file-{file}','application/octet-stream',b'attachment');writer.add_metadata('source',{'number':str(file)});writer.finish()
        path=work/f'input-{file}.mcap';path.write_bytes(data.getvalue());sources.append(path)
    job=work/'job';output=work/'result.mcap';flag=work/'stopped.json'
    preload=work/'pause.cjs';preload.write_text(r'''const fs=require('node:fs');const rename=fs.renameSync;
fs.renameSync=function(a,b){const result=rename.apply(this,arguments);if(String(b).endsWith('checkpoint.bin')){const bytes=fs.readFileSync(b);if(bytes.subarray(0,8).toString('hex')==='434a4f42320d0a00'){let p=12+bytes.readUInt32LE(8);const n=bytes.readUInt32LE(p);if(n){const m=bytes.subarray(p+4,p+4+n);let at=8+65;const count=m.readUInt32LE(at);at+=4;for(let i=0;i<count+1;i++){const length=m.readUInt32LE(at);at+=4+length;}const groups=m.readUInt32LE(at+12);if(groups>=1){fs.writeFileSync(process.env.PIPELINE_FLAG,JSON.stringify({groups}));Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);}}}}return result;};''',encoding='utf-8')
    args=[*sources,'--job',job,'--output',output,'--checkpoint-final','--run-bytes','1024','--merge-bytes','16384','--fan-in','2','--compression','zstd']
    child=subprocess.Popen([os.environ.get('NODE','node'),'--require',str(preload),str(cli),*map(str,args)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env={**os.environ,'PIPELINE_FLAG':str(flag)})
    try:
        deadline=time.monotonic()+180
        while not flag.exists() and child.poll() is None and time.monotonic()<deadline:time.sleep(.01)
        assert flag.exists(),child.communicate(timeout=5)
    finally:
        if child.poll() is None:child.kill()
        child.communicate(timeout=10)
    assert not output.exists()
    checkpoint=(job/'checkpoint.bin').read_bytes();assert checkpoint[:8]==b'CJOB2\r\n\0'
    at=12+struct.unpack_from('<I',checkpoint,8)[0];n=struct.unpack_from('<I',checkpoint,at)[0];merge=checkpoint[at+4:at+4+n]
    original_ids=committed_run_ids(merge);mapped=[10000+i if i>=0 else 10000+32768+(-i-1) for i in original_ids]
    parts={p.name:(hashlib.sha256(p.read_bytes()).hexdigest(),p.stat().st_mtime_ns) for p in job.glob('*.run') if int(p.stem)<10000}
    def run(entry,*values):return subprocess.run([os.environ.get('NODE','node'),str(entry),*map(str,values)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=180)
    status=run(maincli,'job-inspect',job);assert status.returncode==0,status.stderr
    progress=json.loads(status.stdout)['progress'];assert progress['final_sort']['merge_groups']>=1
    clone=work/'corrupt-job';shutil.copytree(job,clone)
    candidate=next(i for i in mapped if 10000<=i<42768);path=clone/f'{candidate}.run';bad=bytearray(path.read_bytes());bad[-1]^=1;path.write_bytes(bad)
    failed=list(args);failed[failed.index(job)]=clone;failed[failed.index(output)]=work/'failed.mcap'
    result=run(cli,*failed,'--resume');assert result.returncode==2 and not (work/'failed.mcap').exists() and 'fingerprint' in result.stderr,result.stderr
    orphan=job/'999999.run';orphan.write_bytes(b'uncommitted')
    result=run(cli,*args,'--resume');assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['engine']=='indexed-chunk-pipeline' and not orphan.exists()
    for name,(digest,mtime) in parts.items():
        path=job/name;assert path.exists() and hashlib.sha256(path.read_bytes()).hexdigest()==digest and path.stat().st_mtime_ns==mtime
    reader=NonSeekingReader(output.open('rb'),validate_crcs=True);rows=list(reader.iter_messages(log_time_order=False))
    wanted=sorted(expected,key=lambda v:(v[0],v[1],v[2]))
    assert [(m.log_time,m.data[0],m.sequence,m.data) for _,_,m in rows]==wanted
    assert len(list(NonSeekingReader(output.open('rb'),validate_crcs=True).iter_attachments()))==2
    assert len(list(NonSeekingReader(output.open('rb'),validate_crcs=True).iter_metadata()))==2
    assert run(maincli,'job-clean',job).returncode==0 and not job.exists()
    report={'result':'passed','actual_process_termination':True,'committed_final_groups_at_stop':progress['final_sort']['merge_groups'],'reused_parts':len(parts),'committed_final_run_ids':mapped,'messages':320,'attachments':2,'metadata':2,'checks':['real final-group crash','run fingerprint rejection','committed part hashes and mtimes unchanged','orphan pruning','official stable message oracle','owned job cleanup']}
    (ROOT/'_interop/chunk-pipeline/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Chunk pipeline workflows passed: final-group process kill/resume, committed part reuse, corrupt-run rejection, stable 320-message oracle')


if __name__=='__main__':main()
