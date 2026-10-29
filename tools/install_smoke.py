"""Install a published module into a fresh consumer and exercise public APIs."""
import argparse,json,os,re,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
SOURCE = r"""///|
fn memory_async(bytes : Bytes) -> @aio.AsyncSource {
  @aio.new_source(bytes.length().to_uint64(), async fn(offset,length) { @async.pause(); bytes[offset.to_int():offset.to_int()+length].to_owned() })
}

///|
async fn smoke() -> Unit {
  let builder = @mcap.new_builder()
  let channel = builder.register_channel("/n", "json")
  for n = 0; n < 32; n = n + 1 { builder.append(channel,n.to_uint64(),@utf8.encode("{\"n\":\{n}}"[:])) }
  let bytes = builder.finish(compression=@mcap.Zstd,messages_per_chunk=4)
  let source = @mcap.memory_source(bytes)
  let fields = @buffer.Buffer::Buffer()
  ignore(@mcap.build_paged_field_index(source,@mcap.new_write_sink(fn(b){fields.write_bytes(b[:])}),["/n"],@nodeio.source_sha256,"moonmcap.std.json-protobuf-cdr.numeric.v1",page_bytes=4096,page_zones=2))
  let disk = @buffer.Buffer::Buffer()
  ignore(@mcap.build_disk_sidecar(source,@nodeio.sort_directory("."),@mcap.new_write_sink(fn(b){disk.write_bytes(b[:])}),@nodeio.source_sha256,page_entries=3))
  let pool = @aio.open_worker_pool("_build/js/debug/build/cmd/worker/worker.js",options={..@aio.worker_options(),min_chunk_bytes:0,min_payload_bytes:0})
  try {
    let plan = @aio.plan_query(memory_async(bytes),index=memory_async(disk.to_bytes()),fields=memory_async(fields.to_bytes()),pool~,filter=@mcap.json_filter("{\"path\":\"/n\",\"op\":\">=\",\"value\":16}"))
    let rows = @aio.open_dataset([plan.query_source()]).cursor({..@mcap.all_query(),offset:2,limit:3}).next_page(10)
    assert_eq(rows.map(fn(r){r.message.log_time}),[18UL,19UL,20UL])
    assert_true(plan.info() is {"content_verified":true,"backend":"MIDX2",..})
    assert_true(pool.metrics() is Object(v) && v["completed"] is Number(n,..) && n>0.0)
  } catch { error => {pool.close();raise error} }
  pool.close()
  let view = @mcap.cdr_decoder("demo/Bytes",b"uint8[] data").view(b"\x00\x01\x00\x00\x04\x00\x00\x00\x01\x02\xfe\xff",selection={..@mcap.decode_selection(),inline_array_items:2})
  assert_eq(view.read_binary("/data",2,2),b"\xfe\xff")
  assert_eq(view.read_elements("/data",2,2).stringify(),"[254,255]")
  assert_eq(@mcap.ros1_decoder("demo/Value",b"byte b\nuint32 n").decode(b"\xff\x78\x56\x34\x12").stringify(),"{\"b\":-1,\"n\":305419896}")
  fn stream(times : Array[UInt64]) -> @mcap.MessageStream {
    let position=[0]
    @mcap.message_stream(fn(){if position[0]==times.length(){return None};let time=times[position[0]];position[0]+=1;Some({channel_id:1,sequence:0U,log_time:time,publish_time:time,data:b"x"})},fn(){()})
  }
  let joined = @mcap.temporal_join(stream([1UL,4UL]),stream([0UL,3UL,5UL]),options={..@mcap.join_options(),tolerance:1UL})
  assert_true(joined.next() is Some(row) && row.right is Some(m) && m.log_time==0UL)
  assert_true(joined.next() is Some(row) && row.right is Some(m) && m.log_time==3UL)
  assert_eq(joined.next(),None)
  let template=@mcap.build_query_index(builder.snapshot()).query_source().declaration_log()
  let options={..@mcap.recording_options(),segment_messages:2,sequences:@mcap.ContinueSequence}
  let directory=@nodeio.open_recording_directory("recording-smoke")
  let session=@mcap.open_recording_session(template,directory.storage(),options~)
  try {for i=0;i<5;i=i+1{session.append(builder.snapshot().messages[i])};ignore(session.finish())} catch{error=>{directory.close();raise error}}
  directory.close()
  let directory=@nodeio.open_recording_directory("recording-smoke",resume=true)
  let resumed=@mcap.open_recording_session(template,directory.storage(),manifest?=directory.manifest(),options~)
  assert_eq(resumed.committed_messages(),5UL);ignore(resumed.finish());directory.close()
  // INCIDENT_SMOKE
  // STREAM_CLIP_SMOKE
  // PAYLOAD_REWRITE_SMOKE
  // SCHEMA_EVOLUTION_SMOKE
  // SYNCHRONIZATION_SMOKE
  // TF_SMOKE
  println("INSTALLED_SMOKE_PASSED: unified worker query, paged fields, binary views, ROS1, temporal join, durable recording")
}

///|
async fn main { smoke() catch { @mcap.Invalid(reason)=>abort(reason); error=>abort("\{error}") } }
"""
PACKAGE = r"""import {
 "LilaLola67/moonmcap" @mcap,
 "LilaLola67/moonmcap/nodeio",
 "LilaLola67/moonmcap/asyncio" @aio,
 "moonbitlang/async",
 "moonbitlang/async/js_async",
 "moonbitlang/core/buffer",
 "moonbitlang/core/encoding/utf8",
}
pkgtype(kind:"executable")
"""
WORKER = r"""///|
fn main { @aio.run_codec_worker() catch {@mcap.Invalid(reason)=>abort(reason)} }
"""
WORKER_PACKAGE = r"""import {"LilaLola67/moonmcap" @mcap,"LilaLola67/moonmcap/asyncio" @aio}
pkgtype(kind:"executable")
"""

INCIDENT = r"""let incident_source = @mcap.build_query_index(builder.snapshot()).query_source()
  let quality = @mcap.check_quality(incident_source, [{..@mcap.quality_rule("/n"),max_gap:Some(1UL)}],0UL,31UL)
  assert_true(quality is Object(q) && q["status"] == Json::string("passed"))
  let events = @mcap.detect_events(incident_source,{..@mcap.event_rule("/n","{\"path\":\"/n\",\"op\":\">=\",\"value\":16}"),max_gap:1UL,duration:2UL})
  assert_eq(events.length(),1);assert_eq(events[0].start,16UL);assert_eq(events[0].confirmed,18UL)
  let clip = @mcap.extract_context_clip(incident_source,16UL,20UL)
  assert_eq(clip.log.messages.length(),5)
  let emitted=[0]
  let retained=[0]
  let capture=@mcap.trigger_recorder(template,fn(log,_){retained[0]=log.messages.length();emitted[0]+=1},options={..@mcap.trigger_options(),pre:2UL,post:2UL})
  for message in builder.snapshot().messages {capture.feed(message);if message.log_time==16UL{capture.trigger("installed")}}
  capture.finish();assert_eq(emitted[0],1);assert_true(retained[0]>0)
  let mapping=@mcap.clock_map([{source:0UL,target:100UL},{source:31UL,target:162UL}])
  let calibrated=incident_source.calibrate_time(mapping).stream(query={..@mcap.all_query(),start_time:132UL,end_time:140UL,offset:1,limit:1})
  assert_true(calibrated.next() is Some(m) && m.log_time==134UL);calibrated.cancel()
"""

STREAM_CLIP = r"""let clip_bytes = @buffer.Buffer::Buffer()
  ignore(@mcap.stream_context_clip(incident_source,@mcap.new_write_sink(fn(b){clip_bytes.write_bytes(b[:])}),1UL,5UL))
  assert_eq(@mcap.read_log(clip_bytes.to_bytes()).messages.length(),5)
  let batch_directory = @nodeio.open_artifact_directory("clip-batch-smoke")
  ignore(@mcap.run_artifact_batch("installed-stream-clips",2,batch_directory.storage(),fn(id,sink){@mcap.stream_context_clip(incident_source,sink,id.to_uint64(),id.to_uint64()+1UL)},stop_after=1))
  batch_directory.close()
  let batch_directory = @nodeio.open_artifact_directory("clip-batch-smoke",resume=true)
  let batch = @mcap.run_artifact_batch("installed-stream-clips",2,batch_directory.storage(),fn(id,sink){@mcap.stream_context_clip(incident_source,sink,id.to_uint64(),id.to_uint64()+1UL)})
  assert_true(batch is Object(fields) && fields["complete"] == Json::boolean(true))
  batch_directory.close()
"""

PAYLOAD_REWRITE = r"""let typed = @mcap.cdr_decoder("demo/Value",b"uint64 counter\nstring frame_id",options={..@mcap.cdr_options(),int64_as_string:true})
  let typed_value : Json = {"counter":"9007199254740993","frame_id":"sensor"}
  assert_eq(typed.decode(typed.encode(typed_value,little_endian=false)),typed_value)
  let edited = @mcap.standard_payload_rewriter().rewrite_message(builder.snapshot().messages[0],template,[{path:"/n",replacement:Some(Json::number(42))}])
  assert_eq(edited.data,b"{\"n\":42}")
  assert_eq(edited.log_time,0UL)
"""

SCHEMA_EVOLUTION = r"""let old_schema : @mcap.Schema = {id:1,name:"demo/Value",encoding:"ros2msg",data:b"uint32 x"}
  let new_schema = {..old_schema,data:b"uint32 renamed"}
  let comparison = @mcap.compare_payload_schemas(old_schema,new_schema)
  assert_true(comparison is Object(fields) && fields["old_reader_new_data"] == Json::boolean(true) && fields["old_query_new_data"] == Json::boolean(false))
  let normalized = incident_source.decode_payload(@mcap.normalizing_decoder(@mcap.standard_payload_decoder(),[{..@mcap.normalized_field("/n","/scaled"),conversion:@mcap.ScaleValue(2.0,0.0)}])).stream(query={..@mcap.all_query(),offset:3,limit:1})
  assert_true(normalized.next() is Some(message) && message.data == b"{\"scaled\":6}")
  normalized.cancel()
"""


SYNCHRONIZATION = r"""let synchronized = @mcap.synchronize([
    @mcap.sync_input("a",stream([10UL,20UL])),
    @mcap.sync_input("b",stream([11UL,21UL])),
    @mcap.sync_input("c",stream([9UL,19UL]))
  ],options={..@mcap.sync_options(),tolerance:1UL})
  assert_true(synchronized.next() is Some(row) && row.cells.length()==3 && row.cells[2].source_time==Some(9UL))
  synchronized.cancel()
"""


TF = r"""let tf = @mcap.tf_tree()
  tf.add({parent:"world",child:"base",time:0UL,transform:@mcap.tf_identity(),is_static:false,origin:None})
  tf.add({parent:"world",child:"base",time:10UL,transform:{..@mcap.tf_identity(),translation:{x:10.0,y:0.0,z:0.0}},is_static:false,origin:None})
  assert_eq(tf.lookup("world","base",5UL).transform.translation.x,5.0)
  assert_eq(tf.context(["base"],2UL,8UL).length(),2)
"""


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version')
    args=parser.parse_args()
    version=args.version or re.search(r'^version = "([^"]+)"',(ROOT/'moon.mod').read_text(encoding='utf-8'),re.M)[1]
    assert re.fullmatch(r'\d+\.\d+\.\d+',version),'expected a published numeric patch version'
    incidents=tuple(map(int,version.split('.'))) >= (0,15,32)
    source=SOURCE.replace('// INCIDENT_SMOKE',INCIDENT if incidents else '')
    stream_clips=tuple(map(int,version.split('.'))) >= (0,16,1)
    rewrites=tuple(map(int,version.split('.'))) >= (0,16,2)
    source=source.replace('// STREAM_CLIP_SMOKE',STREAM_CLIP if stream_clips else '')
    source=source.replace('// PAYLOAD_REWRITE_SMOKE',PAYLOAD_REWRITE if rewrites else '')
    evolution=tuple(map(int,version.split('.'))) >= (0,16,3)
    source=source.replace('// SCHEMA_EVOLUTION_SMOKE',SCHEMA_EVOLUTION if evolution else '')
    synchronization=tuple(map(int,version.split('.'))) >= (0,16,4)
    source=source.replace('// SYNCHRONIZATION_SMOKE',SYNCHRONIZATION if synchronization else '')
    tf=tuple(map(int,version.split('.'))) >= (0,16,5)
    source=source.replace('// TF_SMOKE',TF if tf else '')
    work=ROOT/'_interop/installed'/uuid.uuid4().hex;worker=work/'cmd/worker';worker.mkdir(parents=True)
    (work/'moon.mod').write_text(f'name = "local/installed-smoke"\nversion = "0.0.1"\nimport {{ "LilaLola67/moonmcap@{version}", "moonbitlang/async@0.20.2", }}\n',encoding='utf-8')
    for path,text in [(work/'main.mbt',source),(work/'moon.pkg',PACKAGE),(worker/'main.mbt',WORKER),(worker/'moon.pkg',WORKER_PACKAGE)]:path.write_text(text,encoding='utf-8')
    moon=os.environ.get('MOON','moon')
    identity=subprocess.run([moon,'version','--all'],cwd=work,capture_output=True,text=True,encoding='utf-8',errors='replace',check=True)
    compiler=re.search(r'moonc v([^\s]+)',identity.stdout)
    assert compiler,'compiler identity missing'
    for name,arguments in [('update',['update']),('build',['build','--target','js']),('run',['run','.','--target','js'])]:
        result=subprocess.run([moon,*arguments],cwd=work,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=300)
        (work/f'{name}.log').write_text(result.stdout+result.stderr,encoding='utf-8')
        if result.returncode:raise RuntimeError(f'installed consumer {name} failed; inspect {work/name}.log: '+(result.stdout+result.stderr)[-2500:])
        if name=='run':assert 'INSTALLED_SMOKE_PASSED' in result.stdout,'missing explicit consumer success marker'
    report={'result':'passed','version':version,'module':'LilaLola67/moonmcap','moonc':compiler[1],'fresh_consumer':True,'source_path_override':False,'own_compiled_worker_entry':True,'features':['unified worker query','paged fields','binary views','ROS1','temporal join','durable recording']}
    if incidents:report['features']+=['quality rules','event detection','context clips','trigger recording','clock maps']
    if stream_clips:report['features']+=['stream context clips','artifact batch resume']
    if rewrites:report['features']+=['typed payload encoding and rewrites']
    if evolution:report['features']+=['directional schema comparison','normalized query views']
    if synchronization:report['features']+=['N-topic synchronization']
    if tf:report['features']+=['historical TF and semantic context']
    (work/'report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (ROOT/'_interop/installed/report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(f"Installed {version} consumer passed: {len(report['features'])} public API workflows and consumer-owned worker")


if __name__=='__main__':main()
