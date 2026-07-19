package example

object VerifyCounts {
  def main(args: Array[String]): Unit = {
    val spark = PolarisSpark.session("run-query")
    spark
      .sql("""
        SELECT
          instrument,
          window_start,
          best_bid_price,
          best_ask_price,
          (best_ask_price - best_bid_price) AS spread_abs,
          (best_ask_price - best_bid_price) / ((best_ask_price + best_bid_price) / 2) AS spread_pct
        FROM orderbook.gold.top_of_book_snapshots
        WHERE best_bid_price IS NOT NULL AND best_ask_price IS NOT NULL
        ORDER BY instrument, window_start
      """)
      .show(50, truncate = false)
    spark.stop()
  }
}
