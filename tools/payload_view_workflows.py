"""Selective decoding against independent ROS and protobuf serializers."""
import hashlib,io,json,os,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'_interop_deps'))
import numpy as np
from rosbags.typesys import get_typestore,Stores,get_types_from_msg
from mcap.writer import Writer,CompressionType
from google.protobuf import descriptor_pb2,descriptor_pool,message_factory


def main():
    work=ROOT/'_interop/payload-views'/uuid.uuid4().hex;work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON','moon'),'build','--target','js'],cwd=ROOT,capture_output=True,check=True)
    def run(tool,*args):return subprocess.run([os.environ.get('NODE','node'),str(ROOT/f'_build/js/debug/build/cmd/{tool}/{tool}.js'),*map(str,args)],cwd=ROOT,capture_output=True,text=True,encoding='utf-8',timeout=120)
    store=get_typestore(Stores.ROS2_HUMBLE);name='demo/msg/Large';definition='uint64 stamp\nuint8[] data\nfloat32[] points\nstring label\n';store.register(get_types_from_msg(definition,name));cls=store.types[name]
    array=np.arange(1048576,dtype=np.uint8);points=np.arange(32768,dtype=np.float32)*.5
    message=cls(9007199254740993,array,points,'keep');schema=store.generate_msgdef(name,ros_version=2)[0].encode();schema_path=work/'schema.msg';schema_path.write_bytes(schema)
    payloads=[]
    for little in (False,True):
        wire=bytes(store.serialize_cdr(message,name,little_endian=little));payloads.append(wire);payload=work/f'{little}.bin';payload.write_bytes(wire);export=work/f'{little}.raw'
        result=run('view',schema_path,payload,name,'--inline',8,'--window','/data',1048570,6,'--window','/points',32764,4,'--export','/data',export);assert result.returncode==0,result.stderr
        value=json.loads(result.stdout);assert value['data']['stamp']==message.stamp and value['data']['data']['elements']==len(array)
        assert value['windows']==[array[-6:].tolist(),points[-4:].tolist()];assert hashlib.sha256(export.read_bytes()).digest()==hashlib.sha256(array.tobytes()).digest()
        result=run('view',schema_path,payload,name,'--field','/stamp');assert result.returncode==0,result.stderr;assert json.loads(result.stdout)['data']=={'stamp':message.stamp}
        bad=work/f'{little}-bad.bin';bad.write_bytes(wire[:-1]);assert run('view',schema_path,bad,name,'--field','/stamp').returncode==2
    for mode in CompressionType:
        data=io.BytesIO();writer=Writer(data,compression=mode,enable_data_crcs=True);writer.start();sid=writer.register_schema(name,'ros2msg',schema);cid=writer.register_channel('/large','cdr',sid)
        for n in range(4):writer.add_message(cid,n,payloads[n%2],n,n)
        writer.finish();path=work/f'{mode.name}.mcap';path.write_bytes(data.getvalue())
        result=run('query',path,'--project-decode','--workers',2,'--where',json.dumps({'path':'/label','op':'=','value':'keep'}),'--select','/stamp','--offset',1,'--limit',2)
        assert result.returncode==0,result.stderr;assert [json.loads(line)['sequence'] for line in result.stdout.splitlines()]==[1,2]
        output=work/f'{mode.name}-failed.ndjson';result=run('query',path,'--project-decode','--where',json.dumps({'path':'/data','op':'contains','value':1}),'--output',output);assert result.returncode==2 and not output.exists()
    fd=descriptor_pb2.FileDescriptorProto(name='view.proto',package='demo',syntax='proto3');model=fd.message_type.add(name='Large')
    for number,name_,kind,label in [(1,'stamp',4,1),(2,'blob',12,1),(3,'samples',13,3)]:model.field.add(number=number,name=name_,type=kind,label=label)
    descriptors=descriptor_pb2.FileDescriptorSet();descriptors.file.add().CopyFrom(fd);desc=work/'view.pb';desc.write_bytes(descriptors.SerializeToString())
    pool=descriptor_pool.DescriptorPool();pool.Add(fd);cls=message_factory.GetMessageClass(pool.FindMessageTypeByName('demo.Large'));sample=cls(stamp=9007199254740993,blob=b'x'*524288,samples=[1]*200000);payload=work/'proto.bin';payload.write_bytes(sample.SerializeToString())
    result=run('view',desc,payload,'demo.Large','--protobuf','--field','/stamp');assert result.returncode==0,result.stderr;assert json.loads(result.stdout)=={'stamp':sample.stamp}
    report={'result':'passed','cdr_endians':2,'binary_bytes':len(array),'points':len(points),'codecs':3,'protobuf_skipped_bytes':len(sample.blob),'protobuf_skipped_repeated':len(sample.samples),'binary_export_sha256':hashlib.sha256(array.tobytes()).hexdigest(),'data_origin':'synthetic independent rosbags and protobuf serializers'}
    (ROOT/'_interop/payload-views/report.json').write_text(json.dumps(report,indent=2)+'\n');print('Payload views passed: 1 MiB binary fields, endian float windows, bounded exports, selective worker queries and protobuf field skipping')


if __name__=='__main__':main()
