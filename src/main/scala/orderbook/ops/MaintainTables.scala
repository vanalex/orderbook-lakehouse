package orderbook.ops

import orderbook.common.{PolarisSpark, Watermark}

import java.time.Instant
import org.apache.iceberg.Table
import org.apache.iceberg.actions.{RewriteDataFiles, RewriteManifests}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/** Phase 7 of data_pipeline_plan.md: periodic maintenance for every Iceberg
  * table this pipeline writes, via Iceberg's Spark maintenance actions:
  *
  *   - `rewriteDataFiles` — compacts the small files that frequent
  *     incremental appends/merges (Phase 6) produce into fewer, larger ones.
  *   - `rewriteManifests` — every commit adds at least one small manifest,
  *     so frequent incremental runs leave many of them, and query planning
  *     has to open each one. Rewriting regroups them into fewer manifests,
  *     clustered by partition.
  *   - `expireSnapshots` — drops snapshots (and any data/metadata files
  *     they alone reference) past a retention window, bounding the storage
  *     and metadata growth from those same frequent commits.
  *   - `deleteOrphanFiles` — sweeps files under the table's location that no
  *     live snapshot references (e.g. left behind by a failed write), past a
  *     conservative age cutoff so files from an in-flight commit aren't swept.
  *
  * Run in that order per table, matching Iceberg's own maintenance guidance:
  * compact data files first, then manifests (so they cover the compacted
  * files), so the snapshot/orphan cleanup that follows sees the
  * post-compaction state.
  *
  * The two rewrites record two things in their own commits, so expiring the
  * older snapshots afterwards doesn't force the next pipeline run into a
  * full read:
  *
  *   - the watermarks (and the streaming sink's batch ids) the table holds
  *     as a sink (`Watermark.carriedProperties`), which would otherwise only
  *     exist on the snapshots being expired;
  *   - which data snapshot they rewrote (`Watermark.DataSnapshotKey`), so a
  *     downstream job whose watermark points at that now-expired snapshot
  *     can resume from the rewrite instead.
  *
  * These are the actions behind the `CALL <catalog>.system.*` procedures;
  * calling them directly is what makes forwarding properties possible.
  *
  * Run with: sbt "runMain orderbook.ops.MaintainTables"
  */
object MaintainTables {

  /** A table to maintain. `compact = false` skips the two rewrites, for a
    * table that's already rewritten in full on every run.
    */
  final case class Managed(table: String, compact: Boolean = true)

  val ManagedTables: Seq[Managed] = Seq(
    Managed("bronze.raw_events"),
    Managed("silver.book_events"),
    Managed("gold.ohlcv_bars_1m"),
    Managed("gold.top_of_book_snapshots"),
    // Overwritten in full by every gold run, so it never has small files
    // or manifests worth regrouping — only old snapshots to expire.
    Managed("gold.book_state", compact = false)
  )

  private def env(name: String, default: String): String = sys.env.getOrElse(name, default)

  private def load(spark: SparkSession, fq: String): Table = Spark3Util.loadIcebergTable(spark, fq)

  /** Runs the maintenance actions against `table` (namespace-qualified,
    * e.g. `"gold.book_state"`, no catalog prefix). `now` is passed in rather
    * than read internally so callers use one consistent instant across every
    * table in a run.
    */
  def maintain(
      spark: SparkSession,
      catalog: String,
      managed: Managed,
      snapshotRetentionHours: Long,
      retainLastSnapshots: Int,
      orphanFileRetentionHours: Long,
      now: Instant,
      minInputFiles: Option[Int] = None
  ): Unit = {
    val fq      = s"$catalog.${managed.table}"
    val actions = SparkActions.get(spark)

    if (!managed.compact) {
      println(s"MaintainTables: $fq is rewritten in full on every run — skipping compaction")
    } else {
      val table   = load(spark, fq)
      val carried = Watermark.carriedProperties(table) ++
        Watermark.lastDataSnapshotId(table).map(id => Watermark.DataSnapshotKey -> id.toString)
      if (carried.nonEmpty) println(s"MaintainTables: recording ${carried.keys.toSeq.sorted.mkString(", ")}")

      // Iceberg only compacts a file group once it has `min-input-files`
      // small files (default 5). Lowering it makes compaction visible on
      // demo-sized data.
      val rewriteData = carried.foldLeft(actions.rewriteDataFiles(load(spark, fq)): RewriteDataFiles) {
        case (a, (k, v)) => a.snapshotProperty(k, v)
      }
      val data = minInputFiles.fold(rewriteData)(n => rewriteData.option("min-input-files", n.toString)).execute()
      println(
        s"MaintainTables: compacted $fq — ${data.rewrittenDataFilesCount()} data file(s) " +
          s"rewritten into ${data.addedDataFilesCount()}"
      )

      val manifests = carried
        .foldLeft(actions.rewriteManifests(load(spark, fq)): RewriteManifests) { case (a, (k, v)) =>
          a.snapshotProperty(k, v)
        }
        .execute()
      println(
        s"MaintainTables: rewrote manifests for $fq — ${manifests.rewrittenManifests().asScala.size} manifest(s) " +
          s"into ${manifests.addedManifests().asScala.size}"
      )
    }

    val snapshotCutoff = now.minusSeconds(snapshotRetentionHours * 3600)
    val expired = actions
      .expireSnapshots(load(spark, fq))
      .expireOlderThan(snapshotCutoff.toEpochMilli)
      .retainLast(retainLastSnapshots)
      .execute()
    println(
      s"MaintainTables: expired snapshots older than $snapshotCutoff for $fq (retaining last $retainLastSnapshots) — " +
        s"deleted ${expired.deletedDataFilesCount()} data file(s), ${expired.deletedManifestsCount()} manifest(s), " +
        s"${expired.deletedManifestListsCount()} manifest list(s)"
    )

    val orphanCutoff = now.minusSeconds(orphanFileRetentionHours * 3600)
    val orphans = actions
      .deleteOrphanFiles(load(spark, fq))
      .olderThan(orphanCutoff.toEpochMilli)
      .execute()
    println(
      s"MaintainTables: removed ${orphans.orphanFileLocations().asScala.size} orphan file(s) " +
        s"older than $orphanCutoff for $fq"
    )
  }

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-maintain-tables")
    val catalog = PolarisSpark.catalogName

    val snapshotRetentionHours   = env("MAINTENANCE_SNAPSHOT_RETENTION_HOURS", "168").toLong
    val retainLastSnapshots      = env("MAINTENANCE_RETAIN_LAST_SNAPSHOTS", "1").toInt
    val orphanFileRetentionHours = env("MAINTENANCE_ORPHAN_FILE_RETENTION_HOURS", "72").toLong
    val minInputFiles            = sys.env.get("MAINTENANCE_MIN_INPUT_FILES").map(_.toInt)
    val now                      = Instant.now()

    ManagedTables.foreach { managed =>
      maintain(spark, catalog, managed, snapshotRetentionHours, retainLastSnapshots, orphanFileRetentionHours, now, minInputFiles)
    }

    spark.stop()
  }
}
