package example

class OrderBookSchemaSpec extends munit.FunSuite {
  test("bronzeRawEvents has the raw feed columns") {
    val names = OrderBookSchema.bronzeRawEvents.fieldNames.toSet
    assertEquals(names, Set("event_type", "instrument", "timestamp", "side", "price", "qty", "seq_no"))
  }

  test("EventType.All matches the five raw feed event kinds") {
    assertEquals(OrderBookSchema.EventType.All, Set("add", "cancel", "modify", "trade", "snapshot"))
  }
}
