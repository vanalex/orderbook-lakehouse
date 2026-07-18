package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/** Creates `orderbook.gold.ohlcv_bars_1m` and
  * `orderbook.gold.top_of_book_snapshots` with the schemas from
  * `OrderBookSchema`, partitioned by `event_date` per Phase 5 of
  * data_pipeline_plan.md. Rerunnable: `createOrReplace()` means the tables
  * always end up matching the schema currently defined in code.
  *
  * Run with: sbt "runMain example.CreateGoldTables"
  */
object CreateGoldTables {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-gold-tables")
    val catalog = PolarisSpark.catalogName

    val tables: Seq[(String, StructType)] = Seq(
      s"$catalog.gold.ohlcv_bars_1m"         -> OrderBookSchema.ohlcvBars1m,
      s"$catalog.gold.top_of_book_snapshots" -> OrderBookSchema.topOfBookSnapshots
    )

    tables.foreach { case (table, schema) =>
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      empty.writeTo(table).using("iceberg").partitionedBy(col("event_date")).createOrReplace()

      println(s"Created $table:")
      spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)
    }

    spark.stop()
  }
}
