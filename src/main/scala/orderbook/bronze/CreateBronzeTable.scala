package orderbook.bronze

import orderbook.common.{OrderBookSchema, PolarisSpark}

import org.apache.spark.sql.{Row, SparkSession}

/** Creates `orderbook.bronze.raw_events` with the schema from
  * `OrderBookSchema`. Rerunnable: `createOrReplace()` means the table always
  * ends up matching the schema currently defined in code.
  *
  * Also drops `bronze.ingested_files`, the file-tracking table earlier
  * versions of `IngestRawEvents` used. The streaming checkpoint replaced it.
  *
  * Recreating bronze empties it, so `make setup` also runs `ClearLanding`,
  * which resets the ingest checkpoint along with the landed files.
  *
  * Run with: sbt "runMain orderbook.bronze.CreateBronzeTable"
  */
object CreateBronzeTable {

  def create(spark: SparkSession, catalog: String): Unit = {
    val table = s"$catalog.bronze.raw_events"
    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bronzeRawEvents)
    empty.writeTo(table).using("iceberg").createOrReplace()

    spark.sql(s"DROP TABLE IF EXISTS $catalog.bronze.ingested_files")

    println(s"Created $table:")
    spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = PolarisSpark.session("orderbook-create-bronze-table")
    create(spark, PolarisSpark.catalogName)
    spark.stop()
  }
}
