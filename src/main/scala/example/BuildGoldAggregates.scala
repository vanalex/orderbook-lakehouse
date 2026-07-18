package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Builds the Phase 5 gold aggregates from `orderbook.silver.book_events`:
  * one-minute OHLCV bars from `trade` events, and top-of-book snapshots from
  * a running per-price-level book built off `add`/`cancel`/`trade` events.
  * Each run recomputes the full aggregate (gold aggregates depend on entire
  * windows of silver data, not just newly arrived rows) and dynamically
  * overwrites the `event_date` partitions the result touches, so reruns over
  * the same silver data are idempotent.
  *
  * Run with: sbt "runMain example.BuildGoldAggregates"
  */
object BuildGoldAggregates {

  /** One-minute OHLCV bars per instrument from silver `trade` events. Pure —
    * no Spark session/IO beyond the input `DataFrame` — so it's unit-testable
    * on small in-memory frames.
    */
  def ohlcvBars(silver: DataFrame): DataFrame = {
    val trades = silver
      .where(col("event_type") === OrderBookSchema.EventType.Trade)
      .withColumn("window_start", window(col("timestamp"), "1 minute").getField("start"))

    val bySeqAsc  = Window.partitionBy("instrument", "window_start").orderBy(col("seq_no").asc)
    val bySeqDesc = Window.partitionBy("instrument", "window_start").orderBy(col("seq_no").desc)

    trades
      .withColumn("rn_asc", row_number().over(bySeqAsc))
      .withColumn("rn_desc", row_number().over(bySeqDesc))
      .groupBy("instrument", "window_start")
      .agg(
        max(when(col("rn_asc") === 1, col("price"))).as("open"),
        max(col("price")).as("high"),
        min(col("price")).as("low"),
        max(when(col("rn_desc") === 1, col("price"))).as("close"),
        sum(col("qty")).as("volume")
      )
      .withColumn("event_date", to_date(col("window_start")))
      .select(OrderBookSchema.ohlcvBars1m.fieldNames.map(col): _*)
  }

  /** Top-of-book snapshots per instrument, sampled every minute, from a
    * running per-`(instrument, side, price)` level book: `add` adds qty,
    * `cancel`/`trade` subtract it. `modify` isn't netted in — see
    * `OrderBookSchema.topOfBookSnapshots` for why. A level's qty is forward
    * -filled across windows where it isn't touched, so a resting order keeps
    * counting toward the top of book until something removes it. Pure,
    * unit-testable like `ohlcvBars`.
    */
  def topOfBookSnapshots(silver: DataFrame): DataFrame = {
    val bookEvents = silver
      .where(
        col("event_type").isInCollection(
          Set(OrderBookSchema.EventType.Add, OrderBookSchema.EventType.Cancel, OrderBookSchema.EventType.Trade)
        )
      )
      .withColumn(
        "qty_delta",
        when(col("event_type") === OrderBookSchema.EventType.Add, col("qty")).otherwise(-col("qty"))
      )
      .withColumn("window_start", window(col("timestamp"), "1 minute").getField("start"))

    val levelBySeq   = Window.partitionBy("instrument", "side", "price").orderBy(col("seq_no").asc)
    val lastInWindow = Window.partitionBy("instrument", "side", "price", "window_start").orderBy(col("seq_no").desc)

    // The level's qty as of its last update within each window it was touched in.
    val levelUpdates = bookEvents
      .withColumn("running_qty", sum(col("qty_delta")).over(levelBySeq))
      .withColumn("rn", row_number().over(lastInWindow))
      .where(col("rn") === 1)
      .select("instrument", "side", "price", "window_start", "running_qty")

    val levels  = levelUpdates.select("instrument", "side", "price").distinct()
    val windows = bookEvents.select("instrument", "window_start").distinct()

    val forwardFill = Window
      .partitionBy("instrument", "side", "price")
      .orderBy("window_start")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Every level gets a row for every window its instrument has, so a level
    // untouched in a given window still carries forward its last known qty.
    val book = levels
      .join(windows, "instrument")
      .join(levelUpdates, Seq("instrument", "side", "price", "window_start"), "left")
      .withColumn("level_qty", last(col("running_qty"), ignoreNulls = true).over(forwardFill))
      .where(col("level_qty").isNotNull && col("level_qty") > 0)

    val bestBid = book
      .where(col("side") === "buy")
      .groupBy("instrument", "window_start")
      .agg(max(col("price")).as("best_bid_price"), max_by(col("level_qty"), col("price")).as("best_bid_qty"))

    val bestAsk = book
      .where(col("side") === "sell")
      .groupBy("instrument", "window_start")
      .agg(min(col("price")).as("best_ask_price"), min_by(col("level_qty"), col("price")).as("best_ask_qty"))

    windows
      .join(bestBid, Seq("instrument", "window_start"), "left")
      .join(bestAsk, Seq("instrument", "window_start"), "left")
      .withColumn("event_date", to_date(col("window_start")))
      .select(OrderBookSchema.topOfBookSnapshots.fieldNames.map(col): _*)
  }

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-build-gold-aggregates")
    val catalog = PolarisSpark.catalogName

    val silverTable    = s"$catalog.silver.book_events"
    val ohlcvTable     = s"$catalog.gold.ohlcv_bars_1m"
    val topOfBookTable = s"$catalog.gold.top_of_book_snapshots"

    val silver = spark.table(silverTable).cache()

    val bars = ohlcvBars(silver)
    bars.writeTo(ohlcvTable).overwritePartitions()
    println(s"Wrote ${bars.count()} OHLCV bars to $ohlcvTable")

    val snapshots = topOfBookSnapshots(silver)
    snapshots.writeTo(topOfBookTable).overwritePartitions()
    println(s"Wrote ${snapshots.count()} top-of-book snapshots to $topOfBookTable")

    spark.stop()
  }
}
