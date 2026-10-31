package example

/** Analytical query: bid-ask spread per instrument/minute, from
  * `gold.top_of_book_snapshots`. The core cost-of-trading signal — watch for
  * spread widening around volatile bars.
  *
  * Run with: sbt "runMain example.SpreadOverTime"
  */
object SpreadOverTime {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-spread-over-time")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        SELECT
          instrument,
          window_start,
          best_bid_price,
          best_ask_price,
          best_ask_price - best_bid_price AS spread_abs,
          (best_ask_price - best_bid_price) / ((best_ask_price + best_bid_price) / 2) AS spread_pct
        FROM $catalog.gold.top_of_book_snapshots
        WHERE best_bid_price IS NOT NULL AND best_ask_price IS NOT NULL
        ORDER BY instrument, window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
