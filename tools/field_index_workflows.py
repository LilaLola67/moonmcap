"""Conservative block summaries: independent predicates, exact numbers and measured skips."""
import hashlib,io,json,os,random,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
from mcap.writer import Writer,CompressionType


def main():
    work=ROOT/'_interop/field-index'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    cli=ROOT/'_build/js/debug/build/cmd/field/field.js'
    def run(*args):return subprocess.run([os.environ.get('NODE','node'),str(cli),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=90)
    reports=[]
    for mode in CompressionType:
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=256,enable_data_crcs=True);writer.start();cid=writer.register_channel('/numbers','json',0);records=[]
        for i in range(512):
            value={'n':9007199254740993+i,'label':f'group-{i//16}'}
            if i%7:value['optional']=None if i%2 else i
            writer.add_message(cid,i,json.dumps(value).encode(),i,i);records.append(value)
        writer.finish();source=work/f'{mode.name}.mcap';source.write_bytes(data.getvalue());index=source.with_suffix('.mfld')
        result=run('build',source,index,'--field','/n','--field','/label','--field','/optional');assert result.returncode==0,result.stderr
        digest=hashlib.sha256(source.read_bytes()).hexdigest()
        tests=[({'path':'/n','op':'>=','value':9007199254741480},lambda v:v['n']>=9007199254741480),({'path':'/label','op':'=','value':'group-30'},lambda v:v['label']=='group-30'),({'not':{'path':'/optional','op':'exists'}},lambda v:'optional' not in v),({'path':'/n','op':'<','value':0},lambda v:False),({'any':[{'path':'/n','op':'=','value':9007199254741000},{'path':'/n','op':'>','value':9007199254741500}]},lambda v:v['n']==9007199254741000 or v['n']>9007199254741500)]
        for descending in (False,True):
            for expression,predicate in tests:
                result=run('query',source,index,'--where',json.dumps(expression),'--select','/n','--offset',2,'--limit',11,*(['--descending'] if descending else []))
                assert result.returncode==0,result.stderr
                rows=[json.loads(line) for line in result.stdout.splitlines()]
                expected=[v for v in (list(reversed(records)) if descending else records) if predicate(v)][2:13]
                assert [r['decoded']['data']['/n'] for r in rows]==[v['n'] for v in expected],(expression,rows,expected)
                stats=json.loads(result.stderr)
                if expression==tests[0][0]:assert stats['skipped_chunks']>stats['total_chunks']*.8,stats
                if expression==tests[3][0]:assert stats['decoded_chunks']==0 and stats['query_source_bytes_read']=='0',stats
        assert hashlib.sha256(source.read_bytes()).hexdigest()==digest
        reports.append({'compression':mode.name,'zones':json.loads(run('build',source,work/f'{mode.name}-copy.mfld','--field','/n').stdout)['zones'],'result':'passed'})
        bad=bytearray(index.read_bytes());bad[-1]^=1;corrupt=work/f'{mode.name}-bad.mfld';corrupt.write_bytes(bad);output=work/f'{mode.name}-failed.ndjson'
        result=run('query',source,corrupt,'--where','true','--output',output);assert result.returncode==2 and not output.exists()
    data=io.BytesIO();writer=Writer(data,chunk_size=256);writer.start();cid=writer.register_channel('/bad','json',0);writer.add_message(cid,0,b'invalid JSON',0);writer.finish();source=work/'unknown.mcap';source.write_bytes(data.getvalue());index=work/'unknown.mfld'
    assert run('build',source,index,'--field','/n').returncode==0
    output=work/'unknown.ndjson';result=run('query',source,index,'--where','{"path":"/n","op":">","value":999999}','--output',output)
    assert result.returncode==2 and not output.exists(),'unknown blocks must retain normal decode failures'
    report={'result':'passed','cases':reports,'predicate_queries':30,'unknown_block_error_preserved':True,'corrupt_index_atomic_failures':3,'integer_precision':'exact UInt64 JSON lexemes','notes':'SHA256 verification occurs before query metrics; summaries are trusted local artifacts, not signed proofs'}
    (ROOT/'_interop/field-index/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Field index workflows passed: 30 independent predicates, 3 codecs, >80% block elimination, unknown/corrupt failures')


if __name__=='__main__':main()
