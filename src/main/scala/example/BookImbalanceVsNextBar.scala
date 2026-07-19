package example

/** Analytical query: order-book imbalance (best bid qty vs. best ask qty)
  * joined against the *next* minute's OHLCV bar return, from
  * `gold.top_of_book_snapshots` and `gold.ohlcv_bars_1m`. A short-horizon
  * predictive-signal check — does imbalance lead price direction?
  *
  * Run with: sbt "runMain example.BookImbalanceVsNextBar"
  */
object BookImbalanceVsNextBar {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-book-imbalance-vs-next-bar")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        WITH imbalance AS (
          SELECT instrument, window_start,
                 (best_bid_qty - best_ask_qty) / NULLIF(best_bid_qty + best_ask_qty, 0) AS imbalance
          FROM $catalog.gold.top_of_book_snapshots
          WHERE best_bid_qty IS NOT NULL AND best_ask_qty IS NOT NULL
        )
        SELECT
          i.instrument, i.window_start AS imbalance_window, i.imbalance,
          b.window_start AS next_bar_window, b.close - b.open AS next_bar_return
        FROM imbalance i
        JOIN $catalog.gold.ohlcv_bars_1m b
          ON b.instrument = i.instrument
         AND b.window_start = i.window_start + INTERVAL 1 MINUTE
        ORDER BY i.instrument, i.window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
