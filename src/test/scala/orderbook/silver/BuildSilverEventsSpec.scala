package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

class BuildSilverEventsSpec extends munit.FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("BuildSilverEventsSpec")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val ts = new Timestamp(1700000000000L)

  private def bronze(rows: Row*) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), OrderBookSchema.bronzeRawEvents)

  test("clean keeps well-formed rows and derives event_date") {
    val df = bronze(Row("add", "BTC-USD", ts, "buy", 100.0, 1.0, 1L))
    val cleaned = BuildSilverEvents.clean(df).collect()

    assertEquals(cleaned.length, 1)
    assertEquals(cleaned.head.getAs[java.sql.Date]("event_date"), java.sql.Date.valueOf("2023-11-14"))
  }

  test("clean keeps snapshot rows with null side/price/qty") {
    val df = bronze(Row("snapshot", "BTC-USD", ts, null, null, null, 1L))
    assertEquals(BuildSilverEvents.clean(df).count(), 1L)
  }

  test("clean drops rows with an unknown event_type") {
    val df = bronze(Row("bogus", "BTC-USD", ts, "buy", 100.0, 1.0, 1L))
    assertEquals(BuildSilverEvents.clean(df).count(), 0L)
  }

  test("clean drops non-snapshot rows with an invalid side") {
    val df = bronze(Row("add", "BTC-USD", ts, "up", 100.0, 1.0, 1L))
    assertEquals(BuildSilverEvents.clean(df).count(), 0L)
  }

  test("clean drops non-snapshot rows missing price or qty") {
    val df = bronze(Row("add", "BTC-USD", ts, "buy", null, 1.0, 1L))
    assertEquals(BuildSilverEvents.clean(df).count(), 0L)
  }

  test("clean drops rows with a null or blank instrument") {
    val df = bronze(Row("add", "", ts, "buy", 100.0, 1.0, 1L))
    assertEquals(BuildSilverEvents.clean(df).count(), 0L)
  }

  test("clean dedupes on (instrument, seq_no)") {
    val df = bronze(
      Row("add", "BTC-USD", ts, "buy", 100.0, 1.0, 1L),
      Row("add", "BTC-USD", ts, "buy", 100.0, 1.0, 1L)
    )
    assertEquals(BuildSilverEvents.clean(df).count(), 1L)
  }

  test("clean does not dedupe across different instruments with the same seq_no") {
    val df = bronze(
      Row("add", "BTC-USD", ts, "buy", 100.0, 1.0, 1L),
      Row("add", "ETH-USD", ts, "buy", 100.0, 1.0, 1L)
    )
    assertEquals(BuildSilverEvents.clean(df).count(), 2L)
  }

  test("checkQuality passes when the drop rate is within the threshold") {
    BuildSilverEvents.checkQuality(bronzeCount = 100, cleanCount = 60, maxDropRate = 0.5)
  }

  test("checkQuality passes on an empty bronze batch") {
    BuildSilverEvents.checkQuality(bronzeCount = 0, cleanCount = 0)
  }

  test("checkQuality fails when the drop rate exceeds the threshold") {
    intercept[IllegalArgumentException] {
      BuildSilverEvents.checkQuality(bronzeCount = 100, cleanCount = 10, maxDropRate = 0.5)
    }
  }
}
