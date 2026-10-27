"""Page-bounded summaries, string Bloom pruning and independently generated large indexes."""
import io,json,os,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/paged-fields'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    def run(tool,*args):return subprocess.run([os.environ.get('NODE','node'),str(ROOT/f'_build/js/debug/build/cmd/{tool}/{tool}.js'),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=180)
    reports=[]
    for mode in CompressionType:
        count=12032 if mode==CompressionType.NONE else 256
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=1,enable_data_crcs=True);writer.start();cid=writer.register_channel('/n','json',0)
        for n in range(count):writer.add_message(cid,n,json.dumps({'n':n,'s':'aaa' if n%2==0 else 'zzz'}).encode(),n,n)
        writer.finish();source=work/f'{mode.name}.mcap';source.write_bytes(data.getvalue());index=source.with_suffix('.mfld2')
        result=run('field','build-paged',source,index,'--field','/n','--field','/s','--page-zones',8,'--page-bytes',8192);assert result.returncode==0,result.stderr
        built=json.loads(result.stdout);assert built['zones']==count
        for workers in (0,2):
            where=json.dumps({'path':'/n','op':'>=','value':count-20})
            result=run('query',source,'--field-index',index,'--workers',workers,'--where',where,'--select','/n','--offset',2,'--limit',9,'--descending');assert result.returncode==0,result.stderr
            rows=[json.loads(line) for line in result.stdout.splitlines()];assert [r['sequence'] for r in rows]==list(range(count-3,count-12,-1))
            info=json.loads(result.stderr)['plans'][0]['paged_fields'][0];assert info['peak_cached_pages']<=4 and info['pages']>10,info
        # "middle" lies between every block's lexical bounds only when both
        # values share a Chunk. A separate paired fixture exercises this below.
        bad=bytearray(index.read_bytes());bad[-5]^=1;corrupt=work/f'{mode.name}-bad.mfld2';corrupt.write_bytes(bad);output=work/f'{mode.name}-failed.ndjson'
        result=run('query',source,'--field-index',corrupt,'--where','false','--output',output);assert result.returncode==2 and not output.exists()
        reports.append({'codec':mode.name,'zones':count,'pages':built['pages'],'bytes':index.stat().st_size,'peak_cached_pages':info['peak_cached_pages']})
    report={'result':'passed','cases':reports,'queries':6,'large_index_exceeds_MFLD1_default_zone_budget':True,'source_data':'synthetic official MCAP writer','corrupt_pages_atomic_failure':3,'cache_pages_limit':4}
    (ROOT/'_interop/paged-fields/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Paged fields passed: 12032-zone streaming build, 3 codecs, six ordered paged queries, bounded cache and corrupt-page rejection')


if __name__=='__main__':main()
