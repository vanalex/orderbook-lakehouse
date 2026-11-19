package orderbook.bronze

import orderbook.common.OrderBookSchema

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class IngestRawEventsSpec extends munit.FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("IngestRawEventsSpec")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val landedWithSource =
    StructType(IngestRawEvents.landedSchema.fields :+ StructField("_source_file", StringType))

  private def landed(rows: Row*) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), landedWithSource)

  test("conform types the landed string columns and keeps the source file") {
    val row = IngestRawEvents
      .conform(landed(Row("add", "BTC-USD", "2023-11-14T22:13:20Z", "buy", "100.12", "3", "7", "s3://b/a.json")))
      .collect()
      .head

    assertEquals(row.getAs[java.sql.Timestamp]("timestamp").toInstant.toString, "2023-11-14T22:13:20Z")
    assertEquals(row.getAs[java.math.BigDecimal]("price").compareTo(new java.math.BigDecimal("100.12")), 0)
    assertEquals(row.getAs[Long]("seq_no"), 7L)
    assertEquals(row.getAs[String]("_source_file"), "s3://b/a.json")
    assert(!row.isNullAt(row.fieldIndex("_ingested_at")))
  }

  test("conform accepts a seq_no written as a JSON double") {
    val row = IngestRawEvents.conform(landed(Row("add", "BTC-USD", "2023-11-14T22:13:20Z", "buy", "1", "1", "7.0", "f"))).collect().head
    assertEquals(row.getAs[Long]("seq_no"), 7L)
  }

  test("conform turns values that don't parse into nulls instead of failing") {
    val row = IngestRawEvents
      .conform(landed(Row("add", "BTC-USD", "not-a-time", "buy", "cheap", "1", null, "f")))
      .collect()
      .head

    assert(row.isNullAt(row.fieldIndex("timestamp")))
    assert(row.isNullAt(row.fieldIndex("price")))
    assert(row.isNullAt(row.fieldIndex("seq_no")))
  }

  test("conform outputs exactly the bronze schema") {
    val conformed = IngestRawEvents.conform(landed(Row("snapshot", "BTC-USD", null, null, null, null, "1", "f")))
    assertEquals(conformed.schema.map(f => f.name -> f.dataType), OrderBookSchema.bronzeRawEvents.map(f => f.name -> f.dataType))
  }
}
