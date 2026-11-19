package orderbook.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

/** The order-book event schema (data-pipeline-plan.md, Phase 1).
  *
  * The raw feed carries five kinds of events, tagged by `event_type`:
  * `add`, `cancel`, `modify`, `trade`, `snapshot`.
  *
  * Prices and quantities are fixed-point decimals, not doubles: gold nets
  * quantities per price level by adding and subtracting them, and with
  * doubles an emptied level can be left at a tiny positive residue (e.g.
  * 0.1 + 0.2 - 0.3) that still counts as resting liquidity.
  */
object OrderBookSchema {

  /** The `event_type` values the raw feed uses. */
  object EventType {
    val Add: String      = "add"
    val Cancel: String   = "cancel"
    val Modify: String   = "modify"
    val Trade: String    = "trade"
    val Snapshot: String = "snapshot"

    val All: Set[String] = Set(Add, Cancel, Modify, Trade, Snapshot)
  }

  /** Type of every price column, from bronze to gold. */
  val Price: DecimalType = DecimalType(18, 8)

  /** Type of every quantity/volume column, from bronze to gold. */
  val Qty: DecimalType = DecimalType(18, 8)

  /** The raw feed's columns, as producers send them (one JSON object per
    * event). `side`/`price`/`qty` are absent for `snapshot` events.
    */
  val feedColumns: Seq[String] = Seq("event_type", "instrument", "timestamp", "side", "price", "qty", "seq_no")

  /** Bronze schema for `orderbook.bronze.raw_events`: append-only, the raw
    * feed's columns typed but every one of them nullable, plus where each row
    * came from.
    *
    * Nullable on purpose: bronze keeps whatever arrived. A value that's
    * missing or won't cast (a bad timestamp, a non-numeric price) becomes
    * null here instead of failing the whole append, and silver's
    * `BuildSilverEvents.isWellFormed` drops the row. With required columns,
    * one bad event would fail every ingest run until someone removed it by
    * hand.
    *
    *   - `_source_file`: the landed file the row was read from.
    *   - `_ingested_at`: when the ingest micro-batch that wrote it ran.
    */
  val bronzeRawEvents: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = true),
      StructField("instrument", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("side", StringType, nullable = true),
      StructField("price", Price, nullable = true),
      StructField("qty", Qty, nullable = true),
      StructField("seq_no", LongType, nullable = true),
      StructField("_source_file", StringType, nullable = true),
      StructField("_ingested_at", TimestampType, nullable = true)
    )
  )

  /** Silver schema for `orderbook.silver.book_events`: the feed's columns
    * validated by `BuildSilverEvents` (malformed rows dropped, deduped on
    * `(instrument, seq_no)`), so the identifying ones are required, plus
    * `event_date`, derived from `timestamp` and used, together with
    * `instrument`, as the table's partition columns (Phase 1's medallion
    * layout). `side` is null for `snapshot` events, as are `price`/`qty`.
    */
  val silverBookEvents: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = false),
      StructField("instrument", StringType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("side", StringType, nullable = true),
      StructField("price", Price, nullable = true),
      StructField("qty", Qty, nullable = true),
      StructField("seq_no", LongType, nullable = false),
      StructField("event_date", DateType, nullable = false)
    )
  )

  /** Gold schema for `orderbook.gold.ohlcv_bars_1m`: one-minute OHLCV bars per
    * instrument, derived from silver `trade` events (Phase 5). Keyed by
    * `(instrument, window_start)`.
    */
  val ohlcvBars1m: StructType = StructType(
    Seq(
      StructField("instrument", StringType, nullable = false),
      StructField("window_start", TimestampType, nullable = false),
      StructField("event_date", DateType, nullable = false),
      StructField("open", Price, nullable = false),
      StructField("high", Price, nullable = false),
      StructField("low", Price, nullable = false),
      StructField("close", Price, nullable = false),
      StructField("volume", Qty, nullable = false)
    )
  )

  /** Gold schema for `orderbook.gold.top_of_book_snapshots`: best bid/ask per
    * instrument at the end of every minute with activity (Phase 5), keyed by
    * `(instrument, window_start)`. Reconstructed from a running
    * per-`(instrument, side, price)` level book built off silver `add`/
    * `cancel`/`trade` events — `add` adds qty, `cancel`/`trade` subtract it.
    * `modify` isn't netted into level totals: its row carries the order's new
    * absolute qty, not a delta, and with no `order_id` to track individual
    * orders across events there's no way to recover what changed. `best_bid_*`
    * / `best_ask_*` are null for a window with no resting liquidity on that
    * side.
    */
  val topOfBookSnapshots: StructType = StructType(
    Seq(
      StructField("instrument", StringType, nullable = false),
      StructField("window_start", TimestampType, nullable = false),
      StructField("event_date", DateType, nullable = false),
      StructField("best_bid_price", Price, nullable = true),
      StructField("best_bid_qty", Qty, nullable = true),
      StructField("best_ask_price", Price, nullable = true),
      StructField("best_ask_qty", Qty, nullable = true)
    )
  )

  /** State table for the running per-price-level book that
    * `BuildGoldAggregates.topOfBookSnapshots` carries across runs (Phase 6):
    * current `qty` for every resting `(instrument, side, price)` level as of
    * the silver snapshot recorded in the table's own commit (see
    * `Watermark`). Levels that net to zero are dropped, so the table only
    * holds the live book. Unpartitioned — small state, not an
    * append-oriented event table.
    */
  val bookState: StructType = StructType(
    Seq(
      StructField("instrument", StringType, nullable = false),
      StructField("side", StringType, nullable = false),
      StructField("price", Price, nullable = false),
      StructField("qty", Qty, nullable = false)
    )
  )

  /** Shapes `df` to `schema`: keeps only the schema's columns (by name, in
    * schema order), adds any missing ones as null, and casts each to the
    * schema's type. A value that doesn't cast becomes null (Spark's default,
    * non-ANSI casts), which is what lets bronze accept anything.
    */
  def align(df: DataFrame, schema: StructType): DataFrame = {
    val present = df.columns.toSet
    df.select(schema.fields.toSeq.map { f =>
      val source = if (present.contains(f.name)) col(f.name) else lit(null)
      source.cast(f.dataType).as(f.name)
    }: _*)
  }
}
