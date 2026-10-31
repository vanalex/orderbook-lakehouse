package example

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import org.apache.spark.sql.SparkSession

/** Phase 7 of data_pipeline_plan.md: periodic maintenance for every Iceberg
  * table this pipeline writes, via Iceberg's Spark maintenance procedures
  * (`CALL <catalog>.system.<procedure>(...)`):
  *
  *   - `rewrite_data_files` — compacts the small files that frequent
  *     incremental appends/merges (Phase 6) produce into fewer, larger ones.
  *   - `expire_snapshots` — drops snapshots (and any data/metadata files
  *     they alone reference) past a retention window, bounding the storage
  *     and metadata growth from those same frequent commits.
  *   - `remove_orphan_files` — sweeps files under the table's data directory
  *     that no live snapshot references (e.g. left behind by a failed
  *     write), past a conservative age cutoff so files from an in-flight
  *     commit aren't swept.
  *
  * Run in that order per table, matching Iceberg's own maintenance guidance:
  * compact first, so the snapshot/orphan cleanup that follows sees the
  * post-compaction state.
  *
  * Run with: sbt "runMain example.MaintainTables"
  */
object MaintainTables {

  val ManagedTables: Seq[String] = Seq(
    "bronze.raw_events",
    "silver.book_events",
    "gold.ohlcv_bars_1m",
    "gold.top_of_book_snapshots",
    "gold.book_state"
  )

  private val TimestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC)

  private def env(name: String, default: String): String = sys.env.getOrElse(name, default)

  /** Runs the three maintenance procedures against `table` (namespace-
    * qualified, e.g. `"gold.book_state"`, no catalog prefix). `now` is
    * passed in rather than read internally so callers use one consistent
    * instant across every table in a run.
    */
  def maintain(
      spark: SparkSession,
      catalog: String,
      table: String,
      snapshotRetentionHours: Long,
      retainLastSnapshots: Int,
      orphanFileRetentionHours: Long,
      now: Instant
  ): Unit = {
    val fq = s"$catalog.$table"

    println(s"MaintainTables: rewriting data files for $fq")
    spark.sql(s"CALL $catalog.system.rewrite_data_files(table => '$table')").show(truncate = false)

    val snapshotCutoff = TimestampFormatter.format(now.minusSeconds(snapshotRetentionHours * 3600))
    println(s"MaintainTables: expiring snapshots older than $snapshotCutoff for $fq (retaining last $retainLastSnapshots)")
    spark
      .sql(
        s"CALL $catalog.system.expire_snapshots(table => '$table', " +
          s"older_than => TIMESTAMP '$snapshotCutoff', retain_last => $retainLastSnapshots)"
      )
      .show(truncate = false)

    val orphanCutoff = TimestampFormatter.format(now.minusSeconds(orphanFileRetentionHours * 3600))
    println(s"MaintainTables: removing orphan files older than $orphanCutoff for $fq")
    spark
      .sql(s"CALL $catalog.system.remove_orphan_files(table => '$table', older_than => TIMESTAMP '$orphanCutoff')")
      .show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-maintain-tables")
    val catalog = PolarisSpark.catalogName

    val snapshotRetentionHours   = env("MAINTENANCE_SNAPSHOT_RETENTION_HOURS", "168").toLong
    val retainLastSnapshots      = env("MAINTENANCE_RETAIN_LAST_SNAPSHOTS", "1").toInt
    val orphanFileRetentionHours = env("MAINTENANCE_ORPHAN_FILE_RETENTION_HOURS", "72").toLong
    val now                      = Instant.now()

    ManagedTables.foreach { table =>
      maintain(spark, catalog, table, snapshotRetentionHours, retainLastSnapshots, orphanFileRetentionHours, now)
    }

    spark.stop()
  }
}
