package example

/** Analytical query: Parkinson range-based volatility estimator (uses each
  * bar's high/low rather than close-to-close returns) per instrument, from
  * `gold.ohlcv_bars_1m`. Cheaper to compute than realized volatility and
  * uses information OHLCV bars already carry.
  *
  * Run with: sbt "runMain example.ParkinsonVolatility"
  */
object ParkinsonVolatility {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-parkinson-volatility")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        SELECT instrument, window_start,
               SQRT(
                 AVG(POWER(LN(high / low), 2)) OVER (
                   PARTITION BY instrument ORDER BY window_start
                   ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                 ) / (4 * LN(2))
               ) AS parkinson_vol_30m
        FROM $catalog.gold.ohlcv_bars_1m
        WHERE high > low
        ORDER BY instrument, window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
