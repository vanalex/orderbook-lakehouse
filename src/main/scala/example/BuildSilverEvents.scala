package example

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Cleans bronze raw events and merges them into `orderbook.silver.book_events`
  * (data_pipeline_plan.md, Phase 4). Casts/derives types, drops malformed
  * rows, dedupes on `(instrument, seq_no)`, and merges the result into
  * silver — safe to rerun, since `MERGE INTO` only inserts events not
  * already present in the target.
  *
  * Incremental (Phase 6): only reads bronze rows appended since the last run
  * (tracked via `Watermark` as a property on the silver table), instead of
  * rescanning all of bronze every time. Falls back to a full read on the
  * first run, when no watermark is recorded yet.
  *
  * Run with: sbt "runMain example.BuildSilverEvents"
  */
object BuildSilverEvents {

  private val KnownSides: Set[String] = Set("buy", "sell")

  /** Rows that don't hold up under the raw feed's own invariants: an unknown
    * `event_type`, a missing required field, a `side` outside buy/sell, or
    * (for non-`snapshot` events) a missing `price`/non-positive `qty`.
    * `snapshot` events carry no side/price/qty by design (see
    * `OrderBookSchema`), so they're exempt from the trade-field checks.
    */
  def isWellFormed: Column = {
    val isSnapshot = col("event_type") === OrderBookSchema.EventType.Snapshot

    val knownEventType  = col("event_type").isInCollection(OrderBookSchema.EventType.All)
    val hasRequired     = col("instrument").isNotNull && length(trim(col("instrument"))) > 0 &&
      col("timestamp").isNotNull && col("seq_no").isNotNull
    val validSide        = isSnapshot || col("side").isInCollection(KnownSides)
    val validTradeFields = isSnapshot || (col("price").isNotNull && col("qty").isNotNull && col("qty") > 0)

    knownEventType && hasRequired && validSide && validTradeFields
  }

  /** Filters malformed rows, dedupes on `(instrument, seq_no)`, and derives
    * `event_date` from `timestamp`. Pure — no Spark session/IO beyond the
    * input `DataFrame` — so it's unit-testable on small in-memory frames.
    */
  def clean(bronze: DataFrame): DataFrame = {
    bronze
      .where(isWellFormed)
      .dropDuplicates("instrument", "seq_no")
      .withColumn("event_date", to_date(col("timestamp")))
      .select(OrderBookSchema.silverBookEvents.fieldNames.map(col): _*)
  }

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

  private val WatermarkName = "bronze_snapshot_id"

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-build-silver-events")
    val catalog = PolarisSpark.catalogName
    val bronzeTable = s"$catalog.bronze.raw_events"
    val silverTable = s"$catalog.silver.book_events"

    val current   = Watermark.currentSnapshotId(spark, bronzeTable)
    val watermark = Watermark.get(spark, silverTable, WatermarkName)

    if (current.isEmpty) {
      println(s"BuildSilverEvents: $bronzeTable has no data yet — nothing to do")
    } else if (watermark == current) {
      println(s"BuildSilverEvents: no new bronze snapshots since watermark ${current.get} — nothing to do")
    } else {
      val bronze = watermark match {
        case Some(w) =>
          spark.read
            .format("iceberg")
            .option("start-snapshot-id", w)
            .option("end-snapshot-id", current.get)
            .load(bronzeTable)
        case None =>
          spark.table(bronzeTable)
      }
      val cleaned = clean(bronze).cache()

      val bronzeCount = bronze.count()
      val cleanCount  = cleaned.count()
      checkQuality(bronzeCount, cleanCount)

      cleaned.createOrReplaceTempView("silver_staged_events")
      spark.sql(s"""
        MERGE INTO $silverTable AS target
        USING silver_staged_events AS source
        ON target.instrument = source.instrument AND target.seq_no = source.seq_no
        WHEN NOT MATCHED THEN INSERT *
      """)

      Watermark.set(spark, silverTable, WatermarkName, current.get)
      println(
        s"Merged $cleanCount of $bronzeCount new bronze events (through snapshot ${current.get}) into $silverTable"
      )
    }

    spark.stop()
  }
}
