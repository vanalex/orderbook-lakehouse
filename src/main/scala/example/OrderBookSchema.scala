package example

import org.apache.spark.sql.types._

/** The order-book event schema (data-pipeline-plan.md, Phase 1).
  *
  * The raw feed carries five kinds of events, tagged by `event_type`:
  * `add`, `cancel`, `modify`, `trade`, `snapshot`.
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

  /** Bronze schema for `orderbook.bronze.raw_events`: append-only, matches the
    * shape of the raw feed with minimal typing. `side` is null for `snapshot`
    * events. Proper casting/validation/dedup happens in silver (Phase 4).
    */
  val bronzeRawEvents: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = false),
      StructField("instrument", StringType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("side", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("qty", DoubleType, nullable = true),
      StructField("seq_no", LongType, nullable = false)
    )
  )

  /** Silver schema for `orderbook.silver.book_events`: bronze's columns
    * (validated by `BuildSilverEvents`, malformed rows dropped, deduped on
    * `(instrument, seq_no)`) plus `event_date`, derived from `timestamp` and
    * used, together with `instrument`, as the table's partition columns
    * (Phase 1's medallion layout).
    */
  val silverBookEvents: StructType = StructType(
    bronzeRawEvents.fields :+ StructField("event_date", DateType, nullable = false)
  )

  /** Gold schema for `orderbook.gold.ohlcv_bars_1m`: one-minute OHLCV bars per
    * instrument, derived from silver `trade` events (Phase 5).
    */
  val ohlcvBars1m: StructType = StructType(
    Seq(
      StructField("instrument", StringType, nullable = false),
      StructField("window_start", TimestampType, nullable = false),
      StructField("event_date", DateType, nullable = false),
      StructField("open", DoubleType, nullable = false),
      StructField("high", DoubleType, nullable = false),
      StructField("low", DoubleType, nullable = false),
      StructField("close", DoubleType, nullable = false),
      StructField("volume", DoubleType, nullable = false)
    )
  )

  /** Gold schema for `orderbook.gold.top_of_book_snapshots`: best bid/ask per
    * instrument, sampled every minute (Phase 5). Reconstructed from a running
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
      StructField("best_bid_price", DoubleType, nullable = true),
      StructField("best_bid_qty", DoubleType, nullable = true),
      StructField("best_ask_price", DoubleType, nullable = true),
      StructField("best_ask_qty", DoubleType, nullable = true)
    )
  )

  /** Tracks which source files `IngestRawEvents` has already appended to
    * `orderbook.bronze.raw_events` (Phase 3), keyed by the file's path as
    * Spark reports it (`input_file_name()`), so re-running against a
    * growing landing directory only reads/appends files not already
    * ingested. Unpartitioned, append-only — one row per ingested file.
    */
  val ingestedFiles: StructType = StructType(
    Seq(
      StructField("path", StringType, nullable = false),
      StructField("ingested_at", TimestampType, nullable = false)
    )
  )

  /** State table for the running per-price-level book that
    * `BuildGoldAggregates.topOfBookSnapshots` carries across runs (Phase 6):
    * current `qty` for every `(instrument, side, price)` level as of the
    * last processed silver snapshot. Persisting this means a resting order
    * posted in an earlier incremental batch still counts toward the top of
    * book in later batches, without re-reading all of silver's history on
    * every run. Unpartitioned — small, single-row-per-level state, not an
    * append-oriented event table.
    */
  val bookState: StructType = StructType(
    Seq(
      StructField("instrument", StringType, nullable = false),
      StructField("side", StringType, nullable = false),
      StructField("price", DoubleType, nullable = false),
      StructField("qty", DoubleType, nullable = false)
    )
  )
}
