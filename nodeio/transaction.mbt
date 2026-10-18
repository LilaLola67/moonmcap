///|
pub struct OutputTransaction {
  target : String
  temporary : String
  key : String
  sink : @mcap.WriteSink
  mut closed : Bool
}

///|
pub fn OutputTransaction::sink(self : OutputTransaction) -> @mcap.WriteSink {
  self.sink
}
