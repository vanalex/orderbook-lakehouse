package example

/** Analytical query: rolling realized volatility (stddev of log returns over
  * a 30-bar window) per instrument, from `gold.ohlcv_bars_1m`. Standard
  * volatility estimate — compare instruments or event_dates.
  *
  * Run with: sbt "runMain example.RealizedVolatility"
  */
object RealizedVolatility {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-realized-volatility")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        WITH returns AS (
          SELECT instrument, window_start,
                 LN(close / LAG(close) OVER (PARTITION BY instrument ORDER BY window_start)) AS log_return
          FROM $catalog.gold.ohlcv_bars_1m
        )
        SELECT instrument, window_start,
               STDDEV(log_return) OVER (
                 PARTITION BY instrument ORDER BY window_start
                 ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
               ) AS realized_vol_30m
        FROM returns
        WHERE log_return IS NOT NULL
        ORDER BY instrument, window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
