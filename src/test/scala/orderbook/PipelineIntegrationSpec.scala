package orderbook

import orderbook.bronze.{CreateBronzeTable, IngestRawEvents}
import orderbook.common.{OrderBookSchema, Watermark}
import orderbook.gold.{BuildGoldAggregates, CreateGoldTables}
import orderbook.ops.{CreateNamespaces, MaintainTables}
import orderbook.silver.{BuildSilverEvents, CreateSilverTable}
import orderbook.tools.GenerateSyntheticEvents

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.reflect.io.Directory

/** Runs the real jobs end to end — ingest, silver, gold, maintenance —
  * against a local Iceberg Hadoop catalog in a temp directory, and checks
  * the properties the incremental design depends on: incremental results
  * equal a full recompute, reruns change nothing, duplicates and bad events
  * are absorbed, a crash mid-run is recovered, and maintenance doesn't force
  * a full re-read.
  */
class PipelineIntegrationSpec extends munit.FunSuite {

  private val catalog     = "lake"
  private val root        = Files.createTempDirectory("orderbook-it")
  private val landing     = root.resolve("landing")
  private val checkpoint  = root.resolve("checkpoint")
  private val instruments = Seq("BTC-USD", "ETH-USD")

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("PipelineIntegrationSpec")
    .master("local[2]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(s"spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(s"spark.sql.catalog.$catalog.type", "hadoop")
    .config(s"spark.sql.catalog.$catalog.warehouse", root.resolve("warehouse").toUri.toString)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private val bronze    = s"$catalog.bronze.raw_events"
  private val silver    = s"$catalog.silver.book_events"
  private val ohlcv     = s"$catalog.gold.ohlcv_bars_1m"
  private val topOfBook = s"$catalog.gold.top_of_book_snapshots"
  private val bookState = s"$catalog.gold.book_state"

  override def afterAll(): Unit = {
    spark.stop()
    new Directory(root.toFile).deleteRecursively()
  }

  /** The equivalent of `make setup`: empty tables, no landed files, no checkpoint. */
  override def beforeEach(context: BeforeEach): Unit = {
    CreateNamespaces.Namespaces.foreach(ns => spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$ns"))
    Seq(bronze, silver, ohlcv, topOfBook, bookState).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t PURGE"))
    CreateNamespaces.create(spark, catalog)
    CreateBronzeTable.create(spark, catalog)
    CreateSilverTable.create(spark, catalog)
    CreateGoldTables.create(spark, catalog)
    Seq(landing, checkpoint).foreach(p => new Directory(p.toFile).deleteRecursively())
    Files.createDirectories(landing)
  }

  // ---- helpers ----

  private var fileCounter = 0

  private def land(lines: Seq[String]): Path = {
    fileCounter += 1
    val file = landing.resolve(f"$fileCounter%05d.json")
    Files.write(file, lines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
  }

  private def landEvents(events: Seq[Row]): Path = land(events.map(GenerateSyntheticEvents.toJsonLine))

  private def feed(count: Int, from: Int): Seq[Row] = GenerateSyntheticEvents.generate(count, instruments, from = from)

  private def ingest(): Unit = IngestRawEvents.run(spark, catalog, landing.toUri.toString, checkpoint.toUri.toString)

  private def runAll(): Unit = {
    ingest()
    BuildSilverEvents.run(spark, catalog)
    BuildGoldAggregates.run(spark, catalog)
  }

  private def count(table: String): Long = spark.table(table).count()

  private def snapshotIds(tables: String*): Seq[Option[Long]] = tables.map(Watermark.currentSnapshotId(spark, _))

  /** Same rows, same multiplicity. */
  private def assertSameRows(actual: DataFrame, expected: DataFrame, clue: String): Unit = {
    val a = actual.cache()
    val e = expected.cache()
    assertEquals(a.count(), e.count(), s"$clue: row count")
    assertEquals(a.exceptAll(e).count(), 0L, s"$clue: rows not expected")
    assertEquals(e.exceptAll(a).count(), 0L, s"$clue: rows missing")
  }

  /** Gold as it should be: computed from all of silver in one go. */
  private def assertGoldMatchesFullRecompute(): Unit = {
    val all                   = spark.table(silver)
    val (snapshots, newState) = BuildGoldAggregates.topOfBookSnapshots(all, spark.createDataFrame(spark.sparkContext.emptyRDD[Row], OrderBookSchema.bookState))
    assertSameRows(spark.table(ohlcv), BuildGoldAggregates.ohlcvBars(all), "ohlcv_bars_1m")
    assertSameRows(spark.table(topOfBook), snapshots, "top_of_book_snapshots")
    assertSameRows(spark.table(bookState), newState, "book_state")
  }

  // ---- tests ----

  private def minuteOf(r: Row): Long = r.getAs[java.sql.Timestamp](2).getTime / 60000

  private def isTrade(r: Row): Boolean = r.getString(0) == OrderBookSchema.EventType.Trade

  test("incremental runs equal a full recompute, even with a minute split across runs") {
    val events = feed(1200, from = 0)
    // Cut the feed at a trade whose instrument already traded earlier in the
    // same minute, so that minute's bar needs trades from both runs.
    val cut = events.indices.drop(300).find { i =>
      isTrade(events(i)) && events.take(i).exists { e =>
        isTrade(e) && e.getString(1) == events(i).getString(1) && minuteOf(e) == minuteOf(events(i))
      }
    }.get
    val splitMinute = minuteOf(events(cut))
    val instrument  = events(cut).getString(1)

    landEvents(events.take(cut))
    runAll()
    landEvents(events.drop(cut))
    runAll()

    val bars = spark.table(ohlcv).where(s"instrument = '$instrument' AND window_start = timestamp_seconds(${splitMinute * 60})")
    assertEquals(bars.count(), 1L, "the split minute has exactly one bar")
    val expectedVolume = events
      .filter(e => isTrade(e) && e.getString(1) == instrument && minuteOf(e) == splitMinute)
      .map(_.getDouble(5))
      .sum
    assertEquals(bars.collect().head.getAs[java.math.BigDecimal]("volume").doubleValue, expectedVolume)

    assertEquals(count(bronze), 1200L)
    assertEquals(count(silver), 1200L)
    assertGoldMatchesFullRecompute()
  }

  test("rerunning every stage with nothing new commits nothing") {
    landEvents(feed(400, from = 0))
    runAll()

    val before = snapshotIds(bronze, silver, ohlcv, topOfBook, bookState)
    runAll()
    assertEquals(snapshotIds(bronze, silver, ohlcv, topOfBook, bookState), before)
  }

  test("a replayed file lands in bronze but is deduped in silver and leaves gold unchanged") {
    val events = feed(400, from = 0)
    landEvents(events)
    runAll()
    landEvents(events)
    runAll()

    assertEquals(count(bronze), 800L)
    assertEquals(count(silver), 400L)
    assertGoldMatchesFullRecompute()
  }

  test("a malformed event doesn't block ingest; silver drops it") {
    val good = feed(200, from = 0).map(GenerateSyntheticEvents.toJsonLine)
    land(
      good ++ Seq(
        """{"event_type":"add","instrument":"BTC-USD","timestamp":"2023-11-14T22:13:20Z","side":"buy","price":1,"qty":1}""",
        """{"event_type":"add","instrument":"BTC-USD","timestamp":"yesterday","side":"buy","price":1,"qty":1,"seq_no":999999}""",
        """{"event_type":"add","instrument":"BTC-USD","timestamp":"2023-11-14T22:13:20Z","side":"buy","price":"cheap","qty":1,"seq_no":999998}""",
        """not json at all"""
      )
    )
    runAll()

    assertEquals(count(bronze), 204L)
    assertEquals(count(silver), 200L)
    assertGoldMatchesFullRecompute()
  }

  test("a gold run that dies before committing book_state is recovered by the next run") {
    val events = feed(900, from = 0)
    landEvents(events.take(300))
    runAll()

    landEvents(events.drop(300))
    ingest()
    BuildSilverEvents.run(spark, catalog)
    intercept[IllegalStateException] {
      BuildGoldAggregates.run(spark, catalog, beforeStateCommit = () => throw new IllegalStateException("simulated crash"))
    }
    // The gold MERGEs committed, book_state and its watermark didn't.
    BuildGoldAggregates.run(spark, catalog)

    assertGoldMatchesFullRecompute()
  }

  test("maintenance keeps the next runs incremental, and their results correct") {
    val events = feed(1500, from = 0)
    events.take(900).grouped(300).foreach { slice =>
      landEvents(slice)
      runAll()
    }

    // As aggressive as the demo: expire everything but the latest snapshot.
    MaintainTables.ManagedTables.foreach { managed =>
      MaintainTables.maintain(spark, catalog, managed, 0, 1, 72, Instant.now(), minInputFiles = Some(2))
    }

    assert(Watermark.get(spark, silver, "bronze_snapshot_id", bronze).isDefined, "silver would re-read all of bronze")
    assert(Watermark.get(spark, bookState, "silver_snapshot_id", silver).isDefined, "gold would re-read all of silver")

    // Nothing new: every stage is a no-op.
    val before = snapshotIds(silver, ohlcv, topOfBook, bookState)
    BuildSilverEvents.run(spark, catalog)
    BuildGoldAggregates.run(spark, catalog)
    assertEquals(snapshotIds(silver, ohlcv, topOfBook, bookState), before)

    landEvents(events.drop(900))
    runAll()

    assertEquals(count(silver), 1500L)
    assertGoldMatchesFullRecompute()
  }
}
