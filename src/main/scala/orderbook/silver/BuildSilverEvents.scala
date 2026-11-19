package orderbook.silver

import orderbook.common.{OrderBookSchema, PolarisSpark, Watermark}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Cleans bronze raw events and merges them into `orderbook.silver.book_events`
  * (data_pipeline_plan.md, Phase 4). Drops malformed rows, dedupes on
  * `(instrument, seq_no)`, derives `event_date`, and merges the result into
  * silver — safe to rerun, since `MERGE INTO` only inserts events not
  * already present in the target.
  *
  * Incremental (Phase 6): only reads bronze rows appended since the last run.
  * The bronze snapshot it read up to is recorded in the MERGE's own commit
  * (`Watermark.withCommit`), so silver's data and its watermark always move
  * together. Falls back to a full read on the first run, or when that bronze
  * snapshot is no longer in bronze's history.
  *
  * Run with: sbt "runMain orderbook.silver.BuildSilverEvents"
  */
object BuildSilverEvents {

  private val KnownSides: Set[String] = Set("buy", "sell")

  /** Rows that don't hold up under the raw feed's own invariants: an unknown
    * `event_type`, a missing required field, a `side` outside buy/sell, or
    * (for non-`snapshot` events) a missing `price`/non-positive `qty`.
    * `snapshot` events carry no side/price/qty by design (see
    * `OrderBookSchema`), so they're exempt from the trade-field checks.
    * Bronze turns values that didn't parse into null, so this also catches
    * bad timestamps, prices and sequence numbers.
    */
  def isWellFormed: Column = {
    val isSnapshot = col("event_type") === OrderBookSchema.EventType.Snapshot

    val knownEventType  = col("event_type").isInCollection(OrderBookSchema.EventType.All)
    val hasRequired     = col("instrument").isNotNull && length(trim(col("instrument"))) > 0 &&
      col("timestamp").isNotNull && col("seq_no").isNotNull
    val validSide        = isSnapshot || col("side").isInCollection(KnownSides)
    val validTradeFields = isSnapshot || (col("price").isNotNull && col("qty").isNotNull && col("qty") > 0)

    // coalesce: a null event_type/side makes the predicate null, not false.
    coalesce(knownEventType && hasRequired && validSide && validTradeFields, lit(false))
  }

  /** Filters malformed rows, dedupes on `(instrument, seq_no)`, and derives
    * `event_date` from `timestamp`. Pure — no Spark session/IO beyond the
    * input `DataFrame` — so it's unit-testable on small in-memory frames.
    */
  def clean(bronze: DataFrame): DataFrame =
    OrderBookSchema.align(
      bronze
        .where(isWellFormed)
        .dropDuplicates("instrument", "seq_no")
        .withColumn("event_date", to_date(col("timestamp"))),
      OrderBookSchema.silverBookEvents
    )

  /** Data-quality gate before the merge commits (Phase 4): fails loudly
    * rather than silently letting a bad batch land in silver.
    * `maxDropRate` bounds how much of a batch `clean` may discard as
    * malformed before that's treated as a source problem rather than
    * ordinary noise.
    */
  def checkQuality(bronzeCount: Long, cleanCount: Long, maxDropRate: Double = 0.5): Unit = {
    require(
      bronzeCount == 0 || cleanCount.toDouble / bronzeCount >= (1 - maxDropRate),
      s"BuildSilverEvents: dropped ${bronzeCount - cleanCount}/$bronzeCount rows as malformed " +
        s"(> ${(maxDropRate * 100).toInt}% threshold) — aborting before merge"
    )
  }

  /** `col IN (...)` over the given dates, or `true` for none. Used as a
    * target-only MERGE condition, it lets Iceberg skip every silver
    * partition outside the batch's dates instead of scanning the whole table.
    */
  private[orderbook] def datePredicate(column: String, dates: Seq[java.sql.Date]): String =
    if (dates.isEmpty) "true"
    else dates.map(d => s"DATE '$d'").mkString(s"$column IN (", ", ", ")")

  private val WatermarkName = "bronze_snapshot_id"

  def run(spark: SparkSession, catalog: String): Unit = {
    val bronzeTable = s"$catalog.bronze.raw_events"
    val silverTable = s"$catalog.silver.book_events"

    val current   = Watermark.currentSnapshotId(spark, bronzeTable)
    val watermark = Watermark.get(spark, silverTable, WatermarkName, bronzeTable)

    if (current.isEmpty) {
      println(s"BuildSilverEvents: $bronzeTable has no data yet — nothing to do")
    } else if (watermark == current) {
      println(s"BuildSilverEvents: no new bronze snapshots since watermark ${current.get} — nothing to do")
    } else {
      val bronze = (watermark match {
        case Some(w) =>
          spark.read
            .format("iceberg")
            .option("start-snapshot-id", w)
            .option("end-snapshot-id", current.get)
            .load(bronzeTable)
        case None =>
          spark.read.format("iceberg").option("snapshot-id", current.get).load(bronzeTable)
      }).cache()
      val cleaned = clean(bronze).cache()

      val bronzeCount = bronze.count()
      val cleanCount  = cleaned.count()
      checkQuality(bronzeCount, cleanCount)

      // Matching on event_date too is safe: a duplicate is the same event
      // sent again, so it has the same timestamp. It's what lets the
      // date list below prune the target scan.
      val dates          = cleaned.select("event_date").distinct().collect().map(_.getDate(0)).toSeq
      val snapshotBefore = Watermark.currentSnapshotId(spark, silverTable)
      cleaned.createOrReplaceTempView("silver_staged_events")
      Watermark.withCommit(WatermarkName -> current.get) {
        spark.sql(s"""
          MERGE INTO $silverTable AS target
          USING silver_staged_events AS source
          ON target.instrument = source.instrument
            AND target.seq_no = source.seq_no
            AND target.event_date = source.event_date
            AND ${datePredicate("target.event_date", dates)}
          WHEN NOT MATCHED THEN INSERT *
        """)
      }

      // An insert-only MERGE commits as an append: its snapshot summary
      // already counts what it inserted, so there's no need to count silver.
      val inserted = Watermark.currentSummary(spark, silverTable).get("added-records").map(_.toLong).getOrElse(0L)
      println(
        s"Read $bronzeCount new bronze events (through snapshot ${current.get}), $cleanCount clean; " +
          s"inserted $inserted into $silverTable, skipped ${cleanCount - inserted} already there"
      )
      println(Watermark.describeCommit(spark, silverTable, snapshotBefore))

      cleaned.unpersist()
      bronze.unpersist()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = PolarisSpark.session("orderbook-build-silver-events")
    run(spark, PolarisSpark.catalogName)
    spark.stop()
  }
}
