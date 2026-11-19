package orderbook.gold

import orderbook.common.{OrderBookSchema, PolarisSpark, Watermark}
import orderbook.silver.BuildSilverEvents

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Builds the Phase 5 gold aggregates from `orderbook.silver.book_events`:
  * one-minute OHLCV bars from `trade` events, and top-of-book snapshots from
  * a running per-price-level book built off `add`/`cancel`/`trade` events.
  *
  * Incremental (Phase 6) and idempotent. Each run reads the silver rows
  * committed since the last run, then:
  *
  *   1. OHLCV: finds the minutes those rows touch and recomputes each of
  *      them from all of silver, so a minute whose trades arrived across
  *      two runs still ends up as one complete bar.
  *   2. Top of book: replays the batch on top of the persisted book
  *      (`gold.book_state`) to get the book at the end of each touched
  *      minute.
  *   3. MERGEs both into gold by `(instrument, window_start)`: a recomputed
  *      minute replaces the row already there instead of adding a second one.
  *   4. Overwrites `gold.book_state` last, recording the silver snapshot it
  *      reflects in that same commit (`Watermark.withCommit`).
  *
  * `book_state` is the only output that isn't safe to recompute (applying a
  * batch's deltas twice would double them), so its commit is the one that
  * carries the watermark. If a run dies before step 4, the next run starts
  * from the same watermark and the same state, recomputes the same minutes,
  * and the MERGEs rewrite them with the same values. If the watermark is
  * unusable (silver recreated, or its snapshot expired by `MaintainTables`),
  * the run replays all of silver from an empty book, which is also correct.
  *
  * Run with: sbt "runMain orderbook.gold.BuildGoldAggregates"
  */
object BuildGoldAggregates {

  private val WatermarkName = "silver_snapshot_id"

  private val Zero: Column = lit(0).cast(OrderBookSchema.Qty)

  private def windowStart: Column = window(col("timestamp"), "1 minute").getField("start")

  private def trades(silver: DataFrame): DataFrame =
    silver.where(col("event_type") === OrderBookSchema.EventType.Trade)

  /** One-minute OHLCV bars per instrument from silver `trade` events. Pure —
    * no Spark session/IO beyond the input `DataFrame` — so it's unit-testable
    * on small in-memory frames. Every minute present in the input must be
    * complete for its bar to be correct; see `tradesInTouchedWindows`.
    */
  def ohlcvBars(silver: DataFrame): DataFrame = {
    val withWindow = trades(silver).withColumn("window_start", windowStart)

    val bySeqAsc  = Window.partitionBy("instrument", "window_start").orderBy(col("seq_no").asc)
    val bySeqDesc = Window.partitionBy("instrument", "window_start").orderBy(col("seq_no").desc)

    val bars = withWindow
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

    OrderBookSchema.align(bars, OrderBookSchema.ohlcvBars1m)
  }

  /** Every trade in `silver` that falls in a minute (per instrument) that
    * `batch` has a trade in. Feeding this to `ohlcvBars` rebuilds each
    * touched minute from all of its trades, including ones an earlier run
    * already saw. Pure.
    */
  def tradesInTouchedWindows(silver: DataFrame, batch: DataFrame): DataFrame = {
    val touched = trades(batch).select(col("instrument"), windowStart.as("window_start")).distinct()
    trades(silver)
      .withColumn("window_start", windowStart)
      .join(touched, Seq("instrument", "window_start"), "left_semi")
      .select(silver.columns.toSeq.map(col): _*)
  }

  /** Top-of-book snapshots per instrument at the end of every minute touched
    * by `silverBatch`, from a running per-`(instrument, side, price)` level
    * book: `add` adds qty, `cancel`/`trade` subtract it. `modify` isn't
    * netted in — see `OrderBookSchema.topOfBookSnapshots` for why.
    * `priorState` (the book before this batch — empty on the first run)
    * seeds each level's starting qty, and a level's qty is forward-filled
    * across windows in this batch where it isn't touched, so a resting order
    * posted in an earlier batch keeps counting toward the top of book.
    * Pure, unit-testable like `ohlcvBars`.
    *
    * Returns `(snapshots, newState)`: `snapshots` are the rows for the
    * windows in this batch; `newState` is the full updated book (every level
    * still resting, not just those touched this batch) to persist for the
    * next run. Levels that net to zero are dropped from it.
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
      .withColumn("window_start", windowStart)

    val priorQty = priorState.select(col("instrument"), col("side"), col("price"), col("qty").as("prior_qty"))

    // Full state to persist: every level ever seen, at its qty after this
    // batch's net deltas, minus the ones that are now empty.
    val batchDelta = bookEvents.groupBy("instrument", "side", "price").agg(sum("qty_delta").as("batch_delta"))
    val newState = priorQty
      .join(batchDelta, Seq("instrument", "side", "price"), "full_outer")
      .select(
        col("instrument"),
        col("side"),
        col("price"),
        (coalesce(col("prior_qty"), Zero) + coalesce(col("batch_delta"), Zero)).as("qty")
      )
      .where(col("qty") > 0)

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
        (coalesce(col("prior_qty"), Zero) + col("batch_running_qty")).as("level_qty")
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
        coalesce(last(col("level_qty"), ignoreNulls = true).over(forwardFill), col("prior_qty"), Zero)
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

    (
      OrderBookSchema.align(snapshots, OrderBookSchema.topOfBookSnapshots),
      OrderBookSchema.align(newState, OrderBookSchema.bookState)
    )
  }

  private def emptyBookState(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bookState)

  /** Upserts `rows` (unique per `(instrument, window_start)`) into `table`:
    * a window already there is replaced, a new one inserted. Only the
    * partitions for the rows' dates are scanned. Returns the number of rows.
    */
  private def mergeByWindow(spark: SparkSession, rows: DataFrame, table: String): Long = {
    val staged = rows.cache()
    val count  = staged.count()
    val dates  = staged.select("event_date").distinct().collect().map(_.getDate(0)).toSeq
    val view   = s"gold_staged_${table.split('.').last}"

    val before = Watermark.currentSnapshotId(spark, table)
    staged.createOrReplaceTempView(view)
    spark.sql(s"""
      MERGE INTO $table AS target
      USING $view AS source
      ON target.instrument = source.instrument
        AND target.window_start = source.window_start
        AND target.event_date = source.event_date
        AND ${BuildSilverEvents.datePredicate("target.event_date", dates)}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    println(s"Merged $count row(s) into $table")
    println(Watermark.describeCommit(spark, table, before))

    staged.unpersist()
    count
  }

  /** `beforeStateCommit` runs after the gold MERGEs and before `book_state`
    * is written; tests use it to simulate a crash at the worst moment.
    */
  def run(spark: SparkSession, catalog: String, beforeStateCommit: () => Unit = () => ()): Unit = {
    val silverTable    = s"$catalog.silver.book_events"
    val ohlcvTable     = s"$catalog.gold.ohlcv_bars_1m"
    val topOfBookTable = s"$catalog.gold.top_of_book_snapshots"
    val bookStateTable = s"$catalog.gold.book_state"

    val current   = Watermark.currentSnapshotId(spark, silverTable)
    val watermark = Watermark.get(spark, bookStateTable, WatermarkName, silverTable)

    if (current.isEmpty) {
      println(s"BuildGoldAggregates: $silverTable has no data yet — nothing to do")
    } else if (watermark == current) {
      println(s"BuildGoldAggregates: no new silver snapshots since watermark ${current.get} — nothing to do")
    } else {
      // Pin every silver read to the same snapshot, so the batch, the
      // recomputed minutes and the recorded watermark all agree.
      val silverAtCurrent = spark.read.format("iceberg").option("snapshot-id", current.get).load(silverTable)
      val silverBatch = (watermark match {
        case Some(w) =>
          spark.read
            .format("iceberg")
            .option("start-snapshot-id", w)
            .option("end-snapshot-id", current.get)
            .load(silverTable)
        case None =>
          silverAtCurrent
      }).cache()

      val batchDates = trades(silverBatch).select("event_date").distinct().collect().map(_.getDate(0)).toSeq
      val silverForBars =
        if (batchDates.isEmpty) silverAtCurrent.limit(0)
        else silverAtCurrent.where(col("event_date").isin(batchDates: _*))
      mergeByWindow(spark, ohlcvBars(tradesInTouchedWindows(silverForBars, silverBatch)), ohlcvTable)

      val priorState = if (watermark.isEmpty) emptyBookState(spark) else spark.table(bookStateTable)
      val (snapshots, newState) = topOfBookSnapshots(silverBatch, priorState)
      mergeByWindow(spark, snapshots, topOfBookTable)

      beforeStateCommit()

      // Materialize before overwriting: newState is computed from the
      // table it's about to replace. A full overwrite (not
      // overwritePartitions, which replaces nothing when the new book is
      // empty) so levels that emptied out really disappear. Coalesced to one
      // file: the book is small, and otherwise each of the joins' shuffle
      // partitions writes its own tiny file.
      val state           = newState.coalesce(1).cache()
      val levels          = state.count()
      val bookStateBefore = Watermark.currentSnapshotId(spark, bookStateTable)
      Watermark.withCommit(WatermarkName -> current.get) {
        state.writeTo(bookStateTable).overwrite(lit(true))
      }
      println(s"Updated $bookStateTable: $levels resting level(s) as of silver snapshot ${current.get}")
      println(Watermark.describeCommit(spark, bookStateTable, bookStateBefore))

      state.unpersist()
      silverBatch.unpersist()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = PolarisSpark.session("orderbook-build-gold-aggregates")
    run(spark, PolarisSpark.catalogName)
    spark.stop()
  }
}
