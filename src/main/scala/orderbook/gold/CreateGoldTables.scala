package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/** Creates `orderbook.gold.ohlcv_bars_1m`, `orderbook.gold.top_of_book_snapshots`
  * (partitioned by `event_date`, Phase 5), and `orderbook.gold.book_state`
  * (unpartitioned running-book state, Phase 6) with the schemas from
  * `OrderBookSchema`. Rerunnable: `createOrReplace()` means the tables always
  * end up matching the schema currently defined in code.
  *
  * Run with: sbt "runMain example.CreateGoldTables"
  */
object CreateGoldTables {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-gold-tables")
    val catalog = PolarisSpark.catalogName

    val partitionedTables: Seq[(String, StructType)] = Seq(
      s"$catalog.gold.ohlcv_bars_1m"         -> OrderBookSchema.ohlcvBars1m,
      s"$catalog.gold.top_of_book_snapshots" -> OrderBookSchema.topOfBookSnapshots
    )

    partitionedTables.foreach { case (table, schema) =>
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      empty.writeTo(table).using("iceberg").partitionedBy(col("event_date")).createOrReplace()

      println(s"Created $table:")
      spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)
    }

    val bookStateTable = s"$catalog.gold.book_state"
    val emptyBookState = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bookState)
    emptyBookState.writeTo(bookStateTable).using("iceberg").createOrReplace()

    println(s"Created $bookStateTable:")
    spark.sql(s"DESCRIBE TABLE $bookStateTable").show(truncate = false)

    spark.stop()
  }
}
