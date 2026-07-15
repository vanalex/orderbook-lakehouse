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
}
