"""Verify bounded streamed clips with an independent reader, including >64 MiB output."""
import hashlib,io,json,os,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType
from mcap.reader import make_reader

def main():
    work=ROOT/'_interop/stream-clips'/uuid.uuid4().hex;work.mkdir(parents=True)
    cli=ROOT/'_build/js/debug/build/cmd/incident/incident.js'
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js','--warn-list','-a'],cwd=ROOT,capture_output=True,check=True)
    commands=0
    def run(source,config,output,status=0):
        nonlocal commands
        commands+=1;cfg=work/f'{commands}.json';cfg.write_text(json.dumps(config),encoding='utf-8')
        p=subprocess.run(['node',str(cli),'clip',str(source),str(cfg),str(output)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=180)
        assert p.returncode==status,(p.stderr,p.stdout)
        return json.loads(p.stdout) if status==0 else None
    for codec in CompressionType:
        source=work/f'{codec.name}.mcap'
        with source.open('wb') as f:
            writer=Writer(f,compression=codec,chunk_size=256*1024,enable_data_crcs=True);writer.start()
            state=writer.register_channel('/state','json',0);sensor=writer.register_channel('/sensor','raw',0)
            writer.add_message(state,0,b'{}',0,0)
            payload=bytes(range(256))*256
            for n in range(1100):writer.add_message(sensor,n+1,payload,n+2,n)
            writer.add_attachment(0,0,'calibration','application/octet-stream',b'calibration')
            writer.add_metadata('source',{'experiment':'synthetic'})
            writer.finish()
        before=hashlib.sha256(source.read_bytes()).hexdigest()
        config={'start':'1','end':'1100','topics':['/sensor'],'context':[{'topic':'/state'}],
                'stream':True,'copy_auxiliary':True,'output_payload_bytes':'80000000'}
        output=work/f'{codec.name}-out.mcap';report=run(source,config,output)
        assert int(report['manifest']['clip']['payload_bytes'])>64*1024*1024
        assert report['manifest']['peak_chunk_bytes']<=256*1024
        with output.open('rb') as f:
            reader=make_reader(f,validate_crcs=True);count=0
            for _,channel,message in reader.iter_messages(log_time_order=False):
                if count==0:assert channel.topic=='/state' and message.data==b'{}'
                else:assert channel.topic=='/sensor' and message.data==payload and message.sequence==count-1
                count+=1
            assert count==1101
        with output.open('rb') as f:assert [a.data for a in make_reader(f).iter_attachments()]==[b'calibration']
        failure=work/f'{codec.name}-failure.mcap'
        run(source,{**config,'output_messages':'10'},failure,status=2);assert not failure.exists()
        run(source,{**config,'context_bytes':1},failure,status=2);assert not failure.exists()
        run(source,config,output,status=2)
        assert hashlib.sha256(source.read_bytes()).hexdigest()==before
    report={'result':'passed','commands':commands,'codecs':3,'messages_per_output':1101,
            'payload_bytes_per_output':1100*65536+2,'independent_reader':'mcap 1.3.0 with CRC validation',
            'checks':['payloads and stable order','auxiliary records','atomic failure','existing output refused','source unchanged'],
            'memory_scope':'reported writer chunk state, not total process RSS','origin':'synthetic independent writer'}
    (ROOT/'_interop/stream-clips/report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print('Stream clip workflows passed: three codecs, >64 MiB payloads, 12 commands and atomic failures')

if __name__=='__main__':main()
