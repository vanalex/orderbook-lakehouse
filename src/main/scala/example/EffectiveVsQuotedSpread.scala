package example

/** Analytical query: effective spread (2x the distance between trade price
  * and the prevailing mid) vs. the quoted spread, from `silver.book_events`
  * joined against `gold.top_of_book_snapshots`. Approximate: it borrows the
  * *preceding minute's* snapshot as a stand-in for the quote at trade time,
  * since gold only samples top-of-book once a minute — a true effective
  * spread needs event-level top-of-book, not this coarse join.
  *
  * Run with: sbt "runMain example.EffectiveVsQuotedSpread"
  */
object EffectiveVsQuotedSpread {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-effective-vs-quoted-spread")
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
          t.instrument, t.trade_ts, t.trade_price,
          (b.best_bid_price + b.best_ask_price) / 2 AS mid_price,
          2 * ABS(t.trade_price - (b.best_bid_price + b.best_ask_price) / 2) AS effective_spread,
          b.best_ask_price - b.best_bid_price AS quoted_spread
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
