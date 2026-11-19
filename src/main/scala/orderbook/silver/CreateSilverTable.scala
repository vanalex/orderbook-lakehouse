package orderbook.silver

import orderbook.common.{OrderBookSchema, PolarisSpark}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

/** Creates `orderbook.silver.book_events` with the schema from
  * `OrderBookSchema`, partitioned by `instrument`/`event_date` per Phase 1's
  * medallion layout. Rerunnable: `createOrReplace()` means the table always
  * ends up matching the schema currently defined in code.
  *
  * Run with: sbt "runMain orderbook.silver.CreateSilverTable"
  */
object CreateSilverTable {

  def create(spark: SparkSession, catalog: String): Unit = {
    val table = s"$catalog.silver.book_events"

    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.silverBookEvents)
    empty.writeTo(table).using("iceberg").partitionedBy(col("instrument"), col("event_date")).createOrReplace()

    println(s"Created $table:")
    spark.sql(s"DESCRIBE TABLE $table").show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = PolarisSpark.session("orderbook-create-silver-table")
    create(spark, PolarisSpark.catalogName)
    spark.stop()
  }
}
