package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/** Creates `orderbook.bronze.raw_events` and `orderbook.bronze.ingested_files`
  * (Phase 3's file-tracking table, so `IngestRawEvents` can tell which source
  * files it's already processed) with the schemas from `OrderBookSchema`.
  * Rerunnable: `createOrReplace()` means both tables always end up matching
  * the schema currently defined in code.
  *
  * Run with: sbt "runMain example.CreateBronzeTable"
  */
object CreateBronzeTable {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-bronze-table")
    val catalog = PolarisSpark.catalogName

    val tables: Seq[(String, StructType)] = Seq(
      s"$catalog.bronze.raw_events"      -> OrderBookSchema.bronzeRawEvents,
      s"$catalog.bronze.ingested_files"  -> OrderBookSchema.ingestedFiles
    )

    tables.foreach { case (table, schema) =>
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      empty.writeTo(table).using("iceberg").createOrReplace()

      println(s"Created $table:")
      spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)
    }

    spark.stop()
  }
}
