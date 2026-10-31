package example

/** Analytical query: minutes with no resting liquidity on one or both sides
  * of the book (`gold.top_of_book_snapshots`), joined against that window's
  * OHLCV volume (`gold.ohlcv_bars_1m`) to see whether thin books coincide
  * with low — or, interestingly, high — volume.
  *
  * Run with: sbt "runMain example.LiquidityDryUpWindows"
  */
object LiquidityDryUpWindows {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-liquidity-dry-up-windows")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        SELECT t.instrument, t.window_start, t.best_bid_price, t.best_ask_price, o.volume
        FROM $catalog.gold.top_of_book_snapshots t
        LEFT JOIN $catalog.gold.ohlcv_bars_1m o
          ON o.instrument = t.instrument AND o.window_start = t.window_start
        WHERE t.best_bid_price IS NULL OR t.best_ask_price IS NULL
           OR t.best_bid_qty < 1e-6 OR t.best_ask_qty < 1e-6
        ORDER BY t.instrument, t.window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
