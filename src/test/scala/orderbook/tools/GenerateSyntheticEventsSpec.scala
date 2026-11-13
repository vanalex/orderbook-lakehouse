package orderbook.tools

import orderbook.common.OrderBookSchema.EventType

import java.sql.Timestamp

class GenerateSyntheticEventsSpec extends munit.FunSuite {

  private val instruments = Seq("BTC-USD", "ETH-USD")
  private val events      = GenerateSyntheticEvents.generate(5000, instruments)

  test("generates exactly the requested count with increasing seq_no") {
    assertEquals(events.length, 5000)
    assertEquals(events.map(_.getLong(6)), (1L to 5000L).toSeq)
  }

  test("is deterministic for a given seed") {
    assertEquals(GenerateSyntheticEvents.generate(500, instruments), GenerateSyntheticEvents.generate(500, instruments))
  }

  test("a slice continues the same feed: next seq_no, later timestamps, same events") {
    val first  = GenerateSyntheticEvents.generate(1000, instruments)
    val second = GenerateSyntheticEvents.generate(1000, instruments, from = 1000)
    assertEquals(second, GenerateSyntheticEvents.generate(2000, instruments).drop(1000))
    assertEquals(second.map(_.getLong(6)), (1001L to 2000L).toSeq)
    assert(second.head.getAs[Timestamp](2).after(first.last.getAs[Timestamp](2)))
  }

  test("re-sending a slice produces exactly the same events") {
    assertEquals(
      GenerateSyntheticEvents.generate(500, instruments, from = 2000),
      GenerateSyntheticEvents.generate(500, instruments, from = 2000)
    )
  }

  test("emits no modify events (gold can't apply them without an order_id)") {
    assert(!events.exists(_.getString(0) == EventType.Modify))
  }

  test("covers enough time for one-minute bars (default 1s per step)") {
    val ts      = events.map(_.getAs[Timestamp](2).getTime)
    val minutes = (ts.max - ts.min) / 60000
    assert(minutes >= 30, s"only $minutes minutes of data")
    assertEquals(ts, ts.sorted)
  }

  test("replaying the feed the way gold does never crosses the book or goes negative") {
    // Same netting as BuildGoldAggregates.topOfBookSnapshots: add +qty,
    // cancel/trade -qty, per (instrument, side, price).
    val levels = scala.collection.mutable.Map.empty[(String, String, Double), Double].withDefaultValue(0.0)

    events.foreach { r =>
      val eventType = r.getString(0)
      if (eventType != EventType.Snapshot) {
        val key   = (r.getString(1), r.getString(3), r.getDouble(4))
        val delta = if (eventType == EventType.Add) r.getDouble(5) else -r.getDouble(5)
        levels(key) += delta
        assert(levels(key) >= -1e-9, s"level $key went negative at seq_no ${r.getLong(6)}")

        val inst = r.getString(1)
        val live = levels.collect { case ((i, s, p), q) if i == inst && q > 1e-9 => (s, p) }
        val bid  = live.collect { case ("buy", p) => p }.maxOption
        val ask  = live.collect { case ("sell", p) => p }.minOption
        for (b <- bid; a <- ask)
          assert(b < a, s"$inst crossed at seq_no ${r.getLong(6)}: bid $b >= ask $a")
      }
    }
  }

  test("produces a realistic mix of trades") {
    val trades = events.count(_.getString(0) == EventType.Trade)
    assert(trades > 300, s"only $trades trades")
  }

  test("toJsonLine writes the raw-feed shape with a UTC timestamp") {
    val row  = org.apache.spark.sql.Row("add", "BTC-USD", new Timestamp(1700000000000L), "buy", 100.5, 3.0, 7L)
    val json = ujson.read(GenerateSyntheticEvents.toJsonLine(row))
    assertEquals(json("event_type").str, "add")
    assertEquals(json("timestamp").str, "2023-11-14T22:13:20Z")
    assertEquals(json("price").num, 100.5)
    assertEquals(json("seq_no").num.toLong, 7L)
  }

  test("toJsonLine leaves out a snapshot's null fields") {
    val snapshot = events.find(_.getString(0) == EventType.Snapshot).get
    val keys     = ujson.read(GenerateSyntheticEvents.toJsonLine(snapshot)).obj.keySet
    assertEquals(keys, Set("event_type", "instrument", "timestamp", "seq_no"))
  }
}
