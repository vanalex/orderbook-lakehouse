package example

/** Analytical query: cancel-to-add ratio per instrument per minute, from
  * `silver.book_events`. High ratios are the textbook flag for
  * quote-stuffing/spoofing-like behavior.
  *
  * Run with: sbt "runMain example.CancelToAddRatio"
  */
object CancelToAddRatio {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-cancel-to-add-ratio")
    val catalog = PolarisSpark.catalogName

    spark
      .sql(s"""
        SELECT
          instrument,
          window(timestamp, '1 minute').start AS window_start,
          SUM(CASE WHEN event_type = 'add' THEN 1 ELSE 0 END) AS add_count,
          SUM(CASE WHEN event_type = 'cancel' THEN 1 ELSE 0 END) AS cancel_count,
          SUM(CASE WHEN event_type = 'cancel' THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN event_type = 'add' THEN 1 ELSE 0 END), 0) AS cancel_to_add_ratio
        FROM $catalog.silver.book_events
        GROUP BY instrument, window(timestamp, '1 minute')
        ORDER BY instrument, window_start
      """)
      .show(100, truncate = false)

    spark.stop()
  }
}
