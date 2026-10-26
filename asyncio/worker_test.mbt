///|
#external
type WorkerFixture

///|
extern "js" fn new_worker_fixture() -> @js_async.Promise[WorkerFixture] =
  #| async()=>{const fs=await import('node:fs/promises'),os=await import('node:os'),p=await import('node:path');const root=await fs.mkdtemp(p.join(os.tmpdir(),'moonmcap-worker-test-')),entry=p.join(root,'entry.cjs');await fs.writeFile(entry,"const {parentPort,threadId}=require('node:worker_threads');parentPort.on('message',m=>setTimeout(()=>{const j=JSON.parse(m.payload);parentPort.postMessage({id:m.id,result:j.data,thread:threadId});},5000));parentPort.postMessage({ready:true});",{flag:'wx'});return {root,entry,close:async()=>{if(p.dirname(root)!==p.resolve(os.tmpdir())||!p.basename(root).startsWith('moonmcap-worker-test-'))throw new Error('unexpected fixture directory');await fs.unlink(entry);await fs.rmdir(root);}};}

///|
extern "js" fn fixture_worker_entry(value : WorkerFixture) -> String =
  #| f=>f.entry

///|
extern "js" fn close_worker_fixture(
  value : WorkerFixture,
) -> @js_async.Promise[Unit] =
  #| f=>f.close()

///|
async test "worker host retains running cancellation reservations and closes all threads" {
  let fixture = new_worker_fixture().wait()
  let pool = @asyncio.open_worker_pool(fixture_worker_entry(fixture), options={
    ..@asyncio.worker_options(),
    workers: 1,
    max_jobs: 1,
  })
  let builder = @mcap.new_builder()
  let channel = builder.register_channel("/json", "json")
  builder.append(channel, 0UL, b"{\"n\":1}")
  let log = builder.snapshot()
  let declarations = { ..log, messages: [], }
  let message = log.messages[0]
  try {
    @async.with_task_group(async fn(group) {
      let task = group.spawn(
        async fn() { pool.decode_message(message, declarations) },
        allow_failure=true,
      )
      @async.sleep(10)
      try pool.decode_message(message, declarations) catch {
        _ => ()
      } noraise {
        _ => fail("worker job count bound ignored")
      }
      task.cancel()
      try task.wait() catch {
        _ => ()
      } noraise {
        _ => fail("worker task cancellation ignored")
      }
      guard pool.metrics() is Object(fields) &&
        fields["reserved_bytes"] is Number(bytes, ..) else {
        fail("worker reservation metrics")
      }
      assert_true(bytes > 0)
    })
    pool.close()
    assert_true(
      pool.metrics()
      is {
        "closed": true,
        "active": 0,
        "reserved_bytes": 0,
        "cancelled": 1,
        ..
      },
    )
    pool.close()
  } catch {
    error => {
      pool.close()
      close_worker_fixture(fixture).wait()
      raise error
    }
  }
  close_worker_fixture(fixture).wait()
}
