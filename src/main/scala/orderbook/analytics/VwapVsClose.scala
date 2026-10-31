package example

/** Analytical query: daily VWAP vs. the plain average close, per instrument,
  * from `gold.ohlcv_bars_1m`. A real execution-quality benchmark once this
  * is fed by actual (not synthetic) trade data.
  *
  * Run with: sbt "runMain example.VwapVsClose"
  */
object VwapVsClose {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-vwap-vs-close")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        SELECT
          instrument, event_date,
          SUM(close * volume) / NULLIF(SUM(volume), 0) AS vwap,
          AVG(close) AS avg_close,
          (SUM(close * volume) / NULLIF(SUM(volume), 0)) - AVG(close) AS vwap_minus_avg_close
        FROM $catalog.gold.ohlcv_bars_1m
        GROUP BY instrument, event_date
        ORDER BY instrument, event_date
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
