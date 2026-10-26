///|
fn main {
  @aio.run_codec_worker() catch {
    @mcap.Invalid(reason) => abort(reason)
  }
}
