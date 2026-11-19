package orderbook.common

class OrderBookSchemaSpec extends munit.FunSuite {
  test("bronzeRawEvents has the raw feed columns plus ingest metadata") {
    assertEquals(
      OrderBookSchema.bronzeRawEvents.fieldNames.toSeq,
      OrderBookSchema.feedColumns ++ Seq("_source_file", "_ingested_at")
    )
  }

  test("bronzeRawEvents accepts nulls in every column") {
    assert(OrderBookSchema.bronzeRawEvents.fields.forall(_.nullable))
  }

  test("EventType.All matches the five raw feed event kinds") {
    assertEquals(OrderBookSchema.EventType.All, Set("add", "cancel", "modify", "trade", "snapshot"))
  }
}
