"""Independent brute-force temporal joins over unordered physical MCAP inputs."""
import hashlib,io,json,os,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/temporal-join'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/join/join.js'
    def run(*args):return subprocess.run([os.environ.get('NODE','node'),str(cli),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=120)
    queries=0
    for mode in CompressionType:
        paths=[];left=[];right=[]
        for file in range(2):
            data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=512,enable_data_crcs=True);writer.start();a=writer.register_channel('/camera','json',0);b=writer.register_channel('/imu','json',0)
            for n in range(128):
                is_left=n%3==0;time=(n*17+file*7)%(61 if is_left else 73)
                writer.add_message(a if is_left else b,time,json.dumps({'file':file,'n':n}).encode(),time,n)
                (left if is_left else right).append((time,file,n))
            writer.finish();path=work/f'{mode.name}-{file}.mcap';path.write_bytes(data.getvalue());paths.append(path)
        left.sort();right.sort();digests=[hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
        for how in ('nearest','asof'):
            for missing in ('keep','drop'):
                expected=[]
                for row in left:
                    candidates=[r for r in right if how=='nearest' or r[0]<=row[0]]
                    best=min(candidates,key=lambda r:(abs(r[0]-row[0]),r[0],r[1],r[2])) if candidates else None
                    if best is not None and abs(best[0]-row[0])>4:best=None
                    if best is None and missing=='drop':continue
                    expected.append((row,best))
                result=run(*paths,'--left','/camera','--right','/imu','--mode',how,'--tolerance',4,'--missing',missing,'--workers',2,'--decode','--offset',7,'--limit',27)
                assert result.returncode==0,result.stderr
                rows=[json.loads(line) for line in result.stdout.splitlines()]
                def key(r):return (int(r['log_time']),r['payload']['data']['file'],int(r['sequence']))
                actual=[(key(row['left']),None if row['right'] is None else key(row['right'])) for row in rows]
                assert actual==expected[7:34],(how,missing,actual,expected[7:34]);queries+=1
                stats=json.loads(result.stderr)['join'];assert stats['peak_retained_bytes']<=stats['buffer_bytes']
        output=work/f'{mode.name}-failed.ndjson';result=run(*paths,'--left','/camera','--right','/imu','--join-buffer-bytes',32,'--output',output);assert result.returncode==2 and not output.exists()
        assert digests==[hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
    report={'result':'passed','queries':queries,'codecs':3,'source_files_per_query':2,'physical_order':'intentionally unordered and overlapping timestamps','oracle':'brute force nearest/asof, stable (time, source, physical order) ties','output_pagination':'after join and missing policy','bounded_failure_atomic':3,'data_origin':'synthetic independent Python MCAP writer'}
    (ROOT/'_interop/temporal-join/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Temporal joins passed: 12 brute-force comparisons, 3 codecs, overlapping multi-file timestamps, global paging and buffer failures')


if __name__=='__main__':main()
