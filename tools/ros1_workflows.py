"""Independent ROS1 serialization and optionally all messages of a pinned real bag conversion."""
import dataclasses,hashlib,io,json,math,os,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
import numpy as np
from rosbags.typesys import get_typestore,Stores,get_types_from_msg
from mcap.writer import Writer,CompressionType
from mcap.reader import make_reader


def plain(value,limit=None):
    if dataclasses.is_dataclass(value):return {f.name:plain(getattr(value,f.name),limit) for f in dataclasses.fields(value) if not f.name.startswith('__') and f.default is dataclasses.MISSING}
    if isinstance(value,np.ndarray):
        if limit is not None and len(value)>limit:return {'$array':True,'elements':len(value),'byte_length':value.nbytes}
        return [plain(v,limit) for v in value]
    if isinstance(value,(list,tuple)):return [plain(v,limit) for v in value]
    if isinstance(value,np.generic):value=value.item()
    if isinstance(value,float) and not math.isfinite(value):return 'NaN' if math.isnan(value) else ('Infinity' if value>0 else '-Infinity')
    return value


def equal(actual,expected):
    if isinstance(expected,dict) and expected.get('$array') is True:
        assert '$binary' in actual and actual['elements']==expected['elements'] and actual['byte_length']==expected['byte_length'],(actual,expected);return
    if isinstance(expected,dict):
        assert set(actual)==set(expected),(actual,expected)
        for k,v in expected.items():equal(actual[k],v)
    elif isinstance(expected,list):
        assert len(actual)==len(expected)
        for a,b in zip(actual,expected):equal(a,b)
    elif isinstance(expected,float):assert math.isclose(actual,expected,rel_tol=1e-7,abs_tol=1e-12),(actual,expected)
    else:assert actual==expected,(actual,expected)


def main():
    work=ROOT/'_interop/ros1'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    def run(tool,*args):return subprocess.run([os.environ.get('NODE','node'),str(ROOT/f'_build/js/debug/build/cmd/{tool}/{tool}.js'),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=180)
    store=get_typestore(Stores.ROS1_NOETIC);name='demo/msg/Sample';definition='byte signedbyte\nchar octet\nuint8 prefix\nuint64 counter\nstring text\ntime stamp\nduration elapsed\nHeader header\nfloat64[] values\nuint8[] data\n';store.register(get_types_from_msg(definition,name));t=store.types
    message=t[name](-128,255,3,9007199254740993,'月球',t['builtin_interfaces/msg/Time'](2147483647,999999999),t['builtin_interfaces/msg/Duration'](-7,999999999),t['std_msgs/msg/Header'](4,t['builtin_interfaces/msg/Time'](3,5),'frame'),np.array([1.5,-2.25],dtype=np.float64),np.array([0,255,7],dtype=np.uint8))
    schema=store.generate_msgdef(name,ros_version=1)[0].encode();wire=bytes(store.serialize_ros1(message,name));schema_path=work/'schema.msg';schema_path.write_bytes(schema);payload=work/'payload.bin';payload.write_bytes(wire)
    result=run('decode','raw-ros1',schema_path,payload,name);assert result.returncode==0,result.stderr;equal(json.loads(result.stdout),plain(message))
    export=work/'data.raw';result=run('view',schema_path,payload,name,'--ros1','--inline',0,'--window','/values',0,2,'--export','/data',export);assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['windows']==[[1.5,-2.25]] and export.read_bytes()==message.data.tobytes()
    for bad in (wire[:-1],wire+b'\0'):
        payload.write_bytes(bad);assert run('decode','raw-ros1',schema_path,payload,name).returncode==2
    payload.write_bytes(wire)
    for mode in CompressionType:
        data=io.BytesIO();writer=Writer(data,compression=mode,chunk_size=1024,enable_data_crcs=True);writer.start(profile='ros1');sid=writer.register_schema(name,'ros1msg',schema);cid=writer.register_channel('/ros1','ros1',sid)
        for n in range(12):
            message.counter=9007199254740993+n;writer.add_message(cid,n,bytes(store.serialize_ros1(message,name)),n,n)
        writer.finish();path=work/f'{mode.name}.mcap';path.write_bytes(data.getvalue())
        result=run('query',path,'--workers',2,'--where',json.dumps({'path':'/counter','op':'>','value':9007199254740999}),'--select','/counter','--offset',1,'--limit',3);assert result.returncode==0,result.stderr
        assert [json.loads(line)['sequence'] for line in result.stdout.splitlines()]==[8,9,10]
    public_count=0;public_topics=[]
    if '--public' in sys.argv:
        source=ROOT/'_interop/public-corpus/driving-real.mcap';assert hashlib.sha256(source.read_bytes()).hexdigest()=='cc941e50af6a0411071abcebafca764e4002898801a01b5c85ea04d4fef161cd'
        with source.open('rb') as stream:
            rows=list(make_reader(stream).iter_messages())
        for schema in {s.id:s for s,_,_ in rows}.values():store.register(get_types_from_msg(schema.data.decode(),schema.name.replace('/msg/','/').replace('/','/msg/',1)))
        result=run('query',source,'--project-decode','--where','true');assert result.returncode==0,result.stderr
        actual=[json.loads(line) for line in result.stdout.splitlines()];assert len(actual)==len(rows)==1606
        for got,(schema,channel,msg) in zip(actual,rows):
            typename=schema.name.replace('/msg/','/').replace('/','/msg/',1)
            equal(got['decoded']['data'],plain(store.deserialize_ros1(msg.data,typename),limit=1024));assert int(got['log_time'])==msg.log_time and got['sequence']==msg.sequence;public_count+=1
        public_topics=sorted({c.topic for _,c,_ in rows})
    report={'result':'passed','synthetic_messages':36,'codecs':3,'aliases':'byte=int8,char=uint8','public_messages':public_count,'public_topics':public_topics,'public_source_sha256':'cc941e50af6a0411071abcebafca764e4002898801a01b5c85ea04d4fef161cd' if public_count else None,'public_origin':'Foxglove demo.bag, explicitly converted preserving topic/time/payload; not redistributed'}
    (ROOT/'_interop/ros1/report.json').write_text(json.dumps(report,indent=2)+'\n');print(f'ROS1 passed: primitive/nested/time/array oracle, 3 compressed queries, {public_count} real payload comparisons')


if __name__=='__main__':main()
