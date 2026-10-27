"""Actual process interruption, stable committed prefixes and idempotent session recovery."""
import hashlib,io,json,os,shutil,struct,subprocess,sys,time,uuid,zlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType
from mcap.reader import make_reader


def load(path):
    raw=path.read_bytes();assert raw[:8]==b'MRSC1\r\n\0';length=struct.unpack_from('<I',raw,8)[0];assert len(raw)==length+16 and zlib.crc32(raw[12:-4])==struct.unpack('<I',raw[-4:])[0]
    return json.loads(raw[12:-4])


def save(path,value):
    data=json.dumps(value,separators=(',',':'),ensure_ascii=False).encode();path.write_bytes(b'MRSC1\r\n\0'+struct.pack('<I',len(data))+data+struct.pack('<I',zlib.crc32(data)))


def main():
    work=ROOT/'_interop/recording-sessions'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/session/session.js'
    def command(*args):return [os.environ.get('NODE','node'),str(cli),*map(str,args)]
    def run(*args):return subprocess.run(command(*args),cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=120)
    data=io.BytesIO();writer=Writer(data,compression=CompressionType.ZSTD,chunk_size=2048,enable_data_crcs=True);writer.start();ids=[writer.register_channel('/a','json',0),writer.register_channel('/b','json',0)]
    count=720
    for n in range(count):writer.add_message(ids[n%2],n*17%97,json.dumps({'n':n,'padding':'x'*256}).encode(),n,n+100)
    writer.finish();source=work/'source.mcap';source.write_bytes(data.getvalue());digest=hashlib.sha256(source.read_bytes()).hexdigest();reports=[]
    for compression in ('none','lz4','zstd'):
        directory=work/compression;flags=['--segment-messages','17','--sequence','continue','--compression',compression]
        process=subprocess.Popen(command('new',directory,source,*flags,'--pace-ms','4'),cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8')
        before=None;deadline=time.monotonic()+30
        try:
            while time.monotonic()<deadline:
                if process.poll() is not None:raise AssertionError(process.communicate())
                try:
                    before=load(directory/'manifest.bin');active=before['active'];partial=directory/f'segment-{active}.part'
                    if int(before['committed_messages'])>=34 and active is not None and partial.exists() and partial.stat().st_size>8:break
                except (OSError,AssertionError,KeyError,ValueError):pass
                time.sleep(.005)
            else:raise AssertionError('recording did not expose a committed prefix and active tail')
            locked=run('inspect',directory);assert locked.returncode==2 and 'locked' in locked.stderr
            process.kill();process.communicate(timeout=10)
        finally:
            if process.poll() is None:process.kill();process.communicate(timeout=10)
        before=load(directory/'manifest.bin');active=before['active'];partial=directory/f'segment-{active}.part';tail=partial.read_bytes();assert not tail.endswith(b'\x89MCAP0\r\n')
        committed={s['ordinal']:(directory/f"segment-{s['ordinal']}.mcap").read_bytes() for s in before['segments']}
        # Preserve the interrupted fixture to test a crash between quarantine
        # hard-link publication and removal of the original partial name.
        double=work/f'{compression}-quarantine-window';shutil.copytree(directory,double);os.link(double/partial.name,double/f'segment-{active}.quarantine')
        result=run('resume',directory,source,*flags);assert result.returncode==0,result.stderr;info=json.loads(result.stdout)
        assert int(info['committed_messages'])==count and info['quarantined_segments']==1 and info['adopted_segments']==0,info
        assert (directory/f'segment-{active}.quarantine').read_bytes()==tail
        for ordinal,raw in committed.items():assert (directory/f'segment-{ordinal}.mcap').read_bytes()==raw
        after=load(directory/'manifest.bin');assert after['clean_stop'] and after['active'] is None
        actual=[];sequences={'/a':0,'/b':0}
        for segment in after['segments']:
            path=directory/f"segment-{segment['ordinal']}.mcap";raw=path.read_bytes();assert hashlib.sha256(raw).hexdigest()==segment['sha256'] and len(raw)==int(segment['bytes'])
            with path.open('rb') as stream:
                for _,channel,message in make_reader(stream).iter_messages(log_time_order=False):
                    n=json.loads(message.data)['n'];assert message.sequence==sequences[channel.topic] and message.log_time==n*17%97 and message.publish_time==n
                    sequences[channel.topic]+=1;actual.append(n)
        assert actual==list(range(count)) and sequences=={'/a':360,'/b':360}
        stable={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('segment-*')}
        repeated=run('resume',directory,source,*flags);assert repeated.returncode==0,repeated.stderr;assert json.loads(repeated.stdout)['adopted_segments']==0
        assert stable=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('segment-*')}
        result=run('resume',double,source,*flags);assert result.returncode==0,result.stderr;assert json.loads(result.stdout)['quarantined_segments']==1
        adopted=work/f'{compression}-sealed-window';shutil.copytree(directory,adopted);rewound=load(adopted/'manifest.bin');last=rewound['segments'].pop();rewound['active']=last['ordinal'];rewound['committed_messages']=str(int(rewound['committed_messages'])-int(last['messages']));rewound['clean_stop']=False;save(adopted/'manifest.bin',rewound)
        os.link(adopted/f"segment-{last['ordinal']}.mcap",adopted/f"segment-{last['ordinal']}.part")
        result=run('resume',adopted,source,*flags);assert result.returncode==0,result.stderr;assert json.loads(result.stdout)['adopted_segments']==1
        assert not (adopted/f"segment-{last['ordinal']}.part").exists()
        bad=work/f'{compression}-corrupt';shutil.copytree(directory,bad);manifest=(bad/'manifest.bin').read_bytes();first=bad/f"segment-{after['segments'][0]['ordinal']}.mcap";raw=bytearray(first.read_bytes());raw[-1]^=1;first.write_bytes(raw)
        result=run('resume',bad,source,*flags);assert result.returncode==2 and (bad/'manifest.bin').read_bytes()==manifest
        result=run('resume',directory,source,*flags,'--segment-span','1');assert result.returncode==2 and 'configuration' in result.stderr
        reports.append({'compression':compression,'committed_before_kill':int(before['committed_messages']),'tail_bytes_preserved':len(tail),'messages_after_resume':count,'segments':len(after['segments']),'incomplete_tail_quarantined':True,'sealed_orphan_adopted':True,'idempotent_resume':True,'live_lock_rejected':True})
    assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
    report={'result':'passed','cases':reports,'actual_process_kills':3,'quarantine_commit_window_recovered':3,'sealed_commit_window_recovered':3,'corrupt_committed_segments_rejected':3,'data_origin':'synthetic independent official MCAP writer','durability_scope':'process interruption; not a power-loss test'}
    (ROOT/'_interop/recording-sessions/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Recording sessions passed: 3 real process kills, stable prefixes, per-channel sequences, quarantine/adoption commit windows and idempotent recovery')


if __name__=='__main__':main()
