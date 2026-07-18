package example

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Builds the Phase 5 gold aggregates from `orderbook.silver.book_events`:
  * one-minute OHLCV bars from `trade` events, and top-of-book snapshots from
  * a running per-price-level book built off `add`/`cancel`/`trade` events.
  *
  * Incremental (Phase 6): only reads silver rows appended since the last run
  * (tracked via `Watermark` as a property on the OHLCV table), instead of
  * rescanning all of silver every time. OHLCV bars and top-of-book snapshots
  * are appended, not merged, on the assumption each run covers a complete
  * batch of new data — a window whose data arrives split across two separate
  * runs would produce two partial rows for that window rather than one
  * correct one; see data_pipeline_plan.md Phase 6 for why that's an accepted
  * limitation here rather than engineered around. Top-of-book state is
  * carried forward across runs via `orderbook.gold.book_state`, so a resting
  * order posted in an earlier batch still counts in later ones.
  *
  * Run with: sbt "runMain example.BuildGoldAggregates"
  */
object BuildGoldAggregates {

  private val WatermarkName = "silver_snapshot_id"

  /** One-minute OHLCV bars per instrument from silver `trade` events. Pure —
    * no Spark session/IO beyond the input `DataFrame` — so it's unit-testable
    * on small in-memory frames.
    */
  def ohlcvBars(silverBatch: DataFrame): DataFrame = {
    val trades = silverBatch
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
    * `OrderBookSchema.topOfBookSnapshots` for why. `priorState` (as of the
    * last run — empty on the first) seeds each level's starting qty, and a
    * level's qty is forward-filled across windows in this batch where it
    * isn't touched, so a resting order posted in an earlier batch keeps
    * counting toward the top of book. Pure, unit-testable like `ohlcvBars`.
    *
    * Returns `(snapshots, newState)`: `snapshots` are the new rows to append
    * for the windows in this batch; `newState` is the full updated book
    * (every level ever seen, not just those touched this batch) to persist
    * for the next run.
    */
  def topOfBookSnapshots(silverBatch: DataFrame, priorState: DataFrame): (DataFrame, DataFrame) = {
    val bookEvents = silverBatch
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

    val priorQty = priorState.select(col("instrument"), col("side"), col("price"), col("qty").as("prior_qty"))

    // Full state to persist: every level ever seen, at its qty after this
    // batch's net deltas (0 for levels untouched by this batch, since the
    // join's missing side coalesces to 0).
    val batchDelta = bookEvents.groupBy("instrument", "side", "price").agg(sum("qty_delta").as("batch_delta"))
    val newState = priorQty
      .join(batchDelta, Seq("instrument", "side", "price"), "full_outer")
      .select(
        col("instrument"),
        col("side"),
        col("price"),
        (coalesce(col("prior_qty"), lit(0.0)) + coalesce(col("batch_delta"), lit(0.0))).as("qty")
      )

    // Windows to emit snapshots for: only those touched in this batch.
    val windows = bookEvents.select("instrument", "window_start").distinct()

    // Levels relevant to this batch's windows: touched this batch, plus any
    // pre-existing level for the same instruments (still resting, even if
    // untouched here).
    val levelBySeq   = Window.partitionBy("instrument", "side", "price").orderBy(col("seq_no").asc)
    val lastInWindow = Window.partitionBy("instrument", "side", "price", "window_start").orderBy(col("seq_no").desc)

    val levelUpdatesAbsolute = bookEvents
      .withColumn("batch_running_qty", sum(col("qty_delta")).over(levelBySeq))
      .withColumn("rn", row_number().over(lastInWindow))
      .where(col("rn") === 1)
      .select("instrument", "side", "price", "window_start", "batch_running_qty")
      .join(priorQty, Seq("instrument", "side", "price"), "left")
      .select(
        col("instrument"),
        col("side"),
        col("price"),
        col("window_start"),
        (coalesce(col("prior_qty"), lit(0.0)) + col("batch_running_qty")).as("level_qty")
      )

    val batchInstruments = windows.select("instrument").distinct()
    val priorLevelsInBatch = priorQty
      .join(batchInstruments, "instrument")
      .select("instrument", "side", "price", "prior_qty")

    val levels = levelUpdatesAbsolute
      .select("instrument", "side", "price")
      .union(priorLevelsInBatch.select("instrument", "side", "price"))
      .distinct()

    val forwardFill = Window
      .partitionBy("instrument", "side", "price")
      .orderBy("window_start")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Every level gets a row for every window its instrument has this batch,
    // so a level untouched in a given window still carries forward its last
    // known qty (falling back to its pre-batch qty before it's first touched).
    val book = levels
      .join(windows, "instrument")
      .join(levelUpdatesAbsolute, Seq("instrument", "side", "price", "window_start"), "left")
      .join(priorLevelsInBatch, Seq("instrument", "side", "price"), "left")
      .withColumn(
        "level_qty_filled",
        coalesce(last(col("level_qty"), ignoreNulls = true).over(forwardFill), col("prior_qty"), lit(0.0))
      )
      .where(col("level_qty_filled") > 0)

    val bestBid = book
      .where(col("side") === "buy")
      .groupBy("instrument", "window_start")
      .agg(max(col("price")).as("best_bid_price"), max_by(col("level_qty_filled"), col("price")).as("best_bid_qty"))

    val bestAsk = book
      .where(col("side") === "sell")
      .groupBy("instrument", "window_start")
      .agg(min(col("price")).as("best_ask_price"), min_by(col("level_qty_filled"), col("price")).as("best_ask_qty"))

    val snapshots = windows
      .join(bestBid, Seq("instrument", "window_start"), "left")
      .join(bestAsk, Seq("instrument", "window_start"), "left")
      .withColumn("event_date", to_date(col("window_start")))
      .select(OrderBookSchema.topOfBookSnapshots.fieldNames.map(col): _*)

    (snapshots, newState)
  }

  private def emptyBookState(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bookState)

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-build-gold-aggregates")
    val catalog = PolarisSpark.catalogName

    val silverTable    = s"$catalog.silver.book_events"
    val ohlcvTable     = s"$catalog.gold.ohlcv_bars_1m"
    val topOfBookTable = s"$catalog.gold.top_of_book_snapshots"
    val bookStateTable = s"$catalog.gold.book_state"

    val current   = Watermark.currentSnapshotId(spark, silverTable)
    val watermark = Watermark.get(spark, ohlcvTable, WatermarkName)

    if (current.isEmpty) {
      println(s"BuildGoldAggregates: $silverTable has no data yet — nothing to do")
    } else if (watermark == current) {
      println(s"BuildGoldAggregates: no new silver snapshots since watermark ${current.get} — nothing to do")
    } else {
      val silverBatch = watermark match {
        case Some(w) =>
          spark.read
            .format("iceberg")
            .option("start-snapshot-id", w)
            .option("end-snapshot-id", current.get)
            .load(silverTable)
        case None =>
          spark.table(silverTable)
      }
      val cached = silverBatch.cache()

      val bars = ohlcvBars(cached)
      bars.writeTo(ohlcvTable).append()
      println(s"Appended ${bars.count()} OHLCV bars to $ohlcvTable")

      val priorState = if (watermark.isEmpty) emptyBookState(spark) else spark.table(bookStateTable)
      val (snapshots, newState) = topOfBookSnapshots(cached, priorState)

      snapshots.writeTo(topOfBookTable).append()
      println(s"Appended ${snapshots.count()} top-of-book snapshots to $topOfBookTable")

      newState.writeTo(bookStateTable).overwritePartitions()
      println(s"Updated $bookStateTable")

      Watermark.set(spark, ohlcvTable, WatermarkName, current.get)
    }

    spark.stop()
  }
}
