package example

import org.apache.spark.sql.Row

/** Creates `orderbook.bronze.raw_events` with the schema from
  * `OrderBookSchema`. Rerunnable: `createOrReplace()` means the table always
  * ends up matching the schema currently defined in code.
  *
  * Run with: sbt "runMain example.CreateBronzeTable"
  */
object CreateBronzeTable {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-bronze-table")
    val catalog = PolarisSpark.catalogName
    val table   = s"$catalog.bronze.raw_events"

    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bronzeRawEvents)
    empty.writeTo(table).using("iceberg").createOrReplace()

    println(s"Created $table:")
    spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)

    spark.stop()
  }
}
