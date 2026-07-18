package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

class BuildGoldAggregatesSpec extends munit.FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("BuildGoldAggregatesSpec")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val minute0 = 1700000000000L
  private val minute1 = minute0 + 60000L

  private def silver(rows: Row*) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), OrderBookSchema.silverBookEvents)

  private def row(
    eventType: String,
    instrument: String,
    millis: Long,
    side: String,
    price: java.lang.Double,
    qty: java.lang.Double,
    seqNo: Long
  ): Row = {
    val ts = new Timestamp(millis)
    Row(eventType, instrument, ts, side, price, qty, seqNo, java.sql.Date.valueOf(ts.toLocalDateTime.toLocalDate))
  }

  // ---- ohlcvBars ----

  test("ohlcvBars derives OHLCV from a single trade") {
    val df = silver(row("trade", "BTC-USD", minute0, "buy", 100.0, 2.0, 1L))
    val bars = BuildGoldAggregates.ohlcvBars(df).collect()

    assertEquals(bars.length, 1)
    val bar = bars.head
    assertEquals(bar.getAs[Double]("open"), 100.0)
    assertEquals(bar.getAs[Double]("high"), 100.0)
    assertEquals(bar.getAs[Double]("low"), 100.0)
    assertEquals(bar.getAs[Double]("close"), 100.0)
    assertEquals(bar.getAs[Double]("volume"), 2.0)
  }

  test("ohlcvBars picks open/close by seq_no order and aggregates high/low/volume") {
    val df = silver(
      row("trade", "BTC-USD", minute0, "buy", 100.0, 1.0, 1L),
      row("trade", "BTC-USD", minute0 + 1000L, "sell", 105.0, 2.0, 2L),
      row("trade", "BTC-USD", minute0 + 2000L, "buy", 98.0, 3.0, 3L)
    )
    val bar = BuildGoldAggregates.ohlcvBars(df).collect().head

    assertEquals(bar.getAs[Double]("open"), 100.0)
    assertEquals(bar.getAs[Double]("close"), 98.0)
    assertEquals(bar.getAs[Double]("high"), 105.0)
    assertEquals(bar.getAs[Double]("low"), 98.0)
    assertEquals(bar.getAs[Double]("volume"), 6.0)
  }

  test("ohlcvBars splits trades in different minutes into separate bars") {
    val df = silver(
      row("trade", "BTC-USD", minute0, "buy", 100.0, 1.0, 1L),
      row("trade", "BTC-USD", minute1, "buy", 110.0, 1.0, 2L)
    )
    assertEquals(BuildGoldAggregates.ohlcvBars(df).count(), 2L)
  }

  test("ohlcvBars ignores non-trade events") {
    val df = silver(row("add", "BTC-USD", minute0, "buy", 100.0, 1.0, 1L))
    assertEquals(BuildGoldAggregates.ohlcvBars(df).count(), 0L)
  }

  // ---- topOfBookSnapshots ----

  test("topOfBookSnapshots reports the best bid from a single resting add") {
    val df = silver(row("add", "BTC-USD", minute0, "buy", 100.0, 5.0, 1L))
    val snap = BuildGoldAggregates.topOfBookSnapshots(df).collect().head

    assertEquals(snap.getAs[Double]("best_bid_price"), 100.0)
    assertEquals(snap.getAs[Double]("best_bid_qty"), 5.0)
    assert(snap.isNullAt(snap.fieldIndex("best_ask_price")))
  }

  test("topOfBookSnapshots picks the highest bid and lowest ask across levels") {
    val df = silver(
      row("add", "BTC-USD", minute0, "buy", 99.0, 1.0, 1L),
      row("add", "BTC-USD", minute0, "buy", 100.0, 2.0, 2L),
      row("add", "BTC-USD", minute0, "sell", 102.0, 3.0, 3L),
      row("add", "BTC-USD", minute0, "sell", 101.0, 4.0, 4L)
    )
    val snap = BuildGoldAggregates.topOfBookSnapshots(df).collect().head

    assertEquals(snap.getAs[Double]("best_bid_price"), 100.0)
    assertEquals(snap.getAs[Double]("best_bid_qty"), 2.0)
    assertEquals(snap.getAs[Double]("best_ask_price"), 101.0)
    assertEquals(snap.getAs[Double]("best_ask_qty"), 4.0)
  }

  test("topOfBookSnapshots nets a full cancel of the only level to no bid") {
    val df = silver(
      row("add", "BTC-USD", minute0, "buy", 100.0, 5.0, 1L),
      row("cancel", "BTC-USD", minute0 + 1000L, "buy", 100.0, 5.0, 2L)
    )
    val snap = BuildGoldAggregates.topOfBookSnapshots(df).collect().head
    assert(snap.isNullAt(snap.fieldIndex("best_bid_price")))
  }

  test("topOfBookSnapshots nets a partial trade down but keeps the level resting") {
    val df = silver(
      row("add", "BTC-USD", minute0, "sell", 100.0, 5.0, 1L),
      row("trade", "BTC-USD", minute0 + 1000L, "sell", 100.0, 2.0, 2L)
    )
    val snap = BuildGoldAggregates.topOfBookSnapshots(df).collect().head
    assertEquals(snap.getAs[Double]("best_ask_price"), 100.0)
    assertEquals(snap.getAs[Double]("best_ask_qty"), 3.0)
  }

  test("topOfBookSnapshots does not net modify events into level qty") {
    val df = silver(
      row("add", "BTC-USD", minute0, "buy", 100.0, 5.0, 1L),
      row("modify", "BTC-USD", minute0 + 1000L, "buy", 100.0, 999.0, 2L)
    )
    val snap = BuildGoldAggregates.topOfBookSnapshots(df).collect().head
    assertEquals(snap.getAs[Double]("best_bid_qty"), 5.0)
  }

  test("topOfBookSnapshots forward-fills a level into a later window it isn't touched in") {
    val df = silver(
      row("add", "BTC-USD", minute0, "buy", 100.0, 5.0, 1L),
      row("add", "BTC-USD", minute1, "sell", 200.0, 1.0, 2L)
    )
    val snaps = BuildGoldAggregates.topOfBookSnapshots(df).collect().sortBy(_.getAs[Timestamp]("window_start"))

    assertEquals(snaps.length, 2)
    assertEquals(snaps(1).getAs[Double]("best_bid_price"), 100.0)
    assertEquals(snaps(1).getAs[Double]("best_bid_qty"), 5.0)
    assertEquals(snaps(1).getAs[Double]("best_ask_price"), 200.0)
  }
}
