///|
#external
type CacheFixture

///|
extern "js" fn cache_fixture() -> @js_async.Promise[CacheFixture] =
  #| async()=>{
  #| const http=await import('node:http'),fs=await import('node:fs/promises'),os=await import('node:os'),path=await import('node:path');
  #| const data=Buffer.alloc(65536);for(let i=0;i<data.length;i++)data[i]=i%256;
  #| const directory=await fs.mkdtemp(path.join(os.tmpdir(),'moonmcap-cache-test-'));
  #| const server=http.createServer((req,res)=>{
  #| const m=/^bytes=(\d+)-(\d+)$/.exec(req.headers.range||'');if(!m){res.writeHead(400);res.end();return;}
  #| const a=Number(m[1]),b=Number(m[2]);const send=()=>{if(res.destroyed)return;res.writeHead(206,{'Content-Range':`bytes ${a}-${b}/${data.length}`,'ETag':'"test-v1"','Content-Length':b-a+1});res.end(data.subarray(a,b+1));};if(a===b&&a===0)send();else setTimeout(send,50);
  #| });await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  #| return {url:'http://127.0.0.1:'+server.address().port, directory, close:async()=>{
  #| server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  #| if(path.dirname(directory)!==path.resolve(os.tmpdir())||!path.basename(directory).startsWith('moonmcap-cache-test-'))throw new Error('unexpected fixture path');
  #| for(const name of await fs.readdir(directory)){if(!/^([a-f0-9]{64}-[a-f0-9]{1,16}\.page|[a-f0-9]{64}-[a-f0-9]{24}\.part|\.moonmcap-cache-(v1|lock))$/.test(name))throw new Error('unexpected fixture entry');await fs.unlink(path.join(directory,name));}await fs.rmdir(directory);
  #| }};
  #| }

///|
extern "js" fn fixture_url(value : CacheFixture) -> String =
  #| f=>f.url

///|
extern "js" fn fixture_directory(value : CacheFixture) -> String =
  #| f=>f.directory

///|
extern "js" fn fixture_close(value : CacheFixture) -> @js_async.Promise[Unit] =
  #| f=>f.close()

///|
async test "HTTP cache deduplicates tasks and isolates task cancellation from source lifetime" {
  let fixture = cache_fixture().wait()
  let cache = @asyncio.open_cached_http(
    fixture_url(fixture),
    fixture_directory(fixture),
    options={
      ..@asyncio.http_cache_options(),
      page_bytes: 4096,
      cache_bytes: 65536,
    },
  )
  try {
    @async.with_task_group(async fn(group) {
      let a = group.spawn(async fn() { cache.source().read_exact(0UL, 8) })
      let b = group.spawn(async fn() { cache.source().read_exact(0UL, 8) })
      assert_eq(a.wait(), b.wait())
    })
    guard cache.metrics() is Object(fields) else { fail("cache metrics") }
    assert_eq(fields["requests"], Json::number(5))
    @async.with_task_group(async fn(group) {
      let task = group.spawn(
        async fn() { cache.source().read_exact(40960UL, 8) },
        allow_failure=true,
      )
      @async.sleep(10)
      task.cancel()
      try task.wait() catch {
        _ => ()
      } noraise {
        _ => fail("cancelled task completed normally")
      }
    })
    assert_eq(
      cache.source().read_exact(40960UL, 8),
      b"\x00\x01\x02\x03\x04\x05\x06\x07",
    )
    @async.with_task_group(async fn(group) {
      let task = group.spawn(
        async fn() { cache.source().read_exact(24576UL, 8) },
        allow_failure=true,
      )
      @async.sleep(10)
      cache.source().cancel()
      try task.wait() catch {
        _ => ()
      } noraise {
        _ => fail("source cancellation ignored")
      }
    })
  } catch {
    error => {
      cache.close()
      fixture_close(fixture).wait()
      raise error
    }
  }
  cache.close()
  cache.close()
  fixture_close(fixture).wait()
}
