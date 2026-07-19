package example

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

class IngestRawEventsSpec extends munit.FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("IngestRawEventsSpec")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val ts = new Timestamp(1700000000000L)

  private def withSourceFile(rows: (String, String)*) = {
    val schema = StructType(
      Seq(
        StructField("event_type", StringType, nullable = false),
        StructField("_source_file", StringType, nullable = false)
      )
    )
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map(Row.fromTuple)), schema)
  }

  private def ingestedFiles(paths: String*) = {
    val schema = StructType(
      Seq(
        StructField("path", StringType, nullable = false),
        StructField("ingested_at", TimestampType, nullable = false)
      )
    )
    spark.createDataFrame(spark.sparkContext.parallelize(paths.map(p => Row(p, ts))), schema)
  }

  test("newFilesOnly keeps all rows when nothing has been ingested yet") {
    val raw     = withSourceFile("add" -> "file:/data/a.json", "add" -> "file:/data/b.json")
    val ingested = ingestedFiles()

    assertEquals(IngestRawEvents.newFilesOnly(raw, ingested, "_source_file").count(), 2L)
  }

  test("newFilesOnly drops rows from an already-ingested file") {
    val raw      = withSourceFile("add" -> "file:/data/a.json", "add" -> "file:/data/b.json")
    val ingested = ingestedFiles("file:/data/a.json")

    val remaining = IngestRawEvents.newFilesOnly(raw, ingested, "_source_file").collect()
    assertEquals(remaining.length, 1)
    assertEquals(remaining.head.getAs[String]("_source_file"), "file:/data/b.json")
  }

  test("newFilesOnly drops everything once every file has been ingested") {
    val raw      = withSourceFile("add" -> "file:/data/a.json")
    val ingested = ingestedFiles("file:/data/a.json", "file:/data/b.json")

    assertEquals(IngestRawEvents.newFilesOnly(raw, ingested, "_source_file").count(), 0L)
  }

  test("conform ignores a tracking column not in the bronze schema") {
    val schema = StructType(
      Seq(
        StructField("event_type", StringType, nullable = false),
        StructField("_source_file", StringType, nullable = false)
      )
    )
    val raw = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("snapshot", "file:/data/a.json"))),
      schema
    )

    val conformed = IngestRawEvents.conform(raw)
    assertEquals(conformed.columns.toSeq, OrderBookSchema.bronzeRawEvents.fieldNames.toSeq)
  }
}
