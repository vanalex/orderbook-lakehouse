package example

/** Analytical query: flags each trade as printing inside, above, or below the
  * quoted top-of-book from the preceding minute (`silver.book_events` joined
  * against `gold.top_of_book_snapshots`) — a coarse proxy for price impact,
  * given only 1-minute top-of-book sampling.
  *
  * Run with: sbt "runMain example.TradeThroughDetection"
  */
object TradeThroughDetection {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-trade-through-detection")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        WITH trades AS (
          SELECT instrument, timestamp AS trade_ts, price AS trade_price,
                 date_trunc('minute', timestamp) AS trade_minute
          FROM $catalog.silver.book_events
          WHERE event_type = 'trade'
        )
        SELECT
          t.instrument, t.trade_ts, t.trade_price, b.best_bid_price, b.best_ask_price,
          CASE
            WHEN t.trade_price > b.best_ask_price THEN 'through_ask'
            WHEN t.trade_price < b.best_bid_price THEN 'through_bid'
            ELSE 'within_spread'
          END AS trade_location
        FROM trades t
        JOIN $catalog.gold.top_of_book_snapshots b
          ON b.instrument = t.instrument
         AND b.window_start = t.trade_minute - INTERVAL 1 MINUTE
        WHERE b.best_bid_price IS NOT NULL AND b.best_ask_price IS NOT NULL
        ORDER BY t.instrument, t.trade_ts
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
