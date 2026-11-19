package orderbook.common

import org.apache.iceberg.Table
import org.apache.iceberg.spark.{CommitMetadata, Spark3Util}
import org.apache.iceberg.util.SnapshotUtil
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/** Tracks "last upstream snapshot id processed" so a job can incrementally
  * scan its source via Iceberg's `start-snapshot-id`/`end-snapshot-id` read
  * instead of rescanning full history every run (Phase 6 of
  * data_pipeline_plan.md).
  *
  * The watermark is stored in the summary of the sink's own snapshot, written
  * in the same commit as the data it describes (`withCommit`). A crash can
  * therefore never leave the data committed but the watermark behind, or the
  * other way round: either both land or neither does.
  *
  * Maintenance commits (compaction, manifest rewrites) would not carry it on
  * their own, so `MaintainTables` forwards it (`carriedProperties`), and
  * `get` looks back through the sink's snapshot ancestry for the newest
  * snapshot that has one.
  */
object Watermark {

  val PropertyPrefix: String = "pipeline.watermark."

  def propertyKey(name: String): String = s"$PropertyPrefix$name"

  /** Snapshot-summary keys Iceberg's streaming sink uses to make a retried
    * micro-batch a no-op (`IngestRawEvents`). Forwarded through maintenance
    * like the watermarks.
    */
  private val StreamingPrefix = "spark.sql.streaming."

  /** Current snapshot id of `table`, or `None` if it has no snapshots yet
    * (freshly created, never written to).
    */
  def currentSnapshotId(spark: SparkSession, table: String): Option[Long] =
    Option(Spark3Util.loadIcebergTable(spark, table).currentSnapshot()).map(_.snapshotId())

  /** The summary of `table`'s current snapshot, empty if it has none. */
  def currentSummary(spark: SparkSession, table: String): Map[String, String] =
    Option(Spark3Util.loadIcebergTable(spark, table).currentSnapshot())
      .map(_.summary().asScala.toMap)
      .getOrElse(Map.empty)

  /** One log line for the snapshot a write committed to `table`, given the
    * table's snapshot id from before the write. If the id hasn't changed,
    * the write committed nothing.
    */
  def describeCommit(spark: SparkSession, table: String, before: Option[Long]): String =
    currentSnapshotId(spark, table) match {
      case after @ Some(id) if after != before =>
        s"Committed snapshot $id to $table (parent ${before.getOrElse("none")})"
      case after =>
        s"No new snapshot on $table (current ${after.getOrElse("none")})"
    }

  /** Snapshot-summary properties to forward from `table`'s history into a
    * new maintenance commit: the newest value of every watermark and
    * streaming key found walking back from the current snapshot.
    */
  def carriedProperties(table: Table): Map[String, String] =
    SnapshotUtil
      .currentAncestors(table)
      .asScala
      .foldLeft(Map.empty[String, String]) { (acc, snapshot) =>
        val forwarded = snapshot.summary().asScala.filter { case (k, _) =>
          (k.startsWith(PropertyPrefix) || k.startsWith(StreamingPrefix)) && !acc.contains(k)
        }
        acc ++ forwarded
      }

  /** The watermark `name` recorded on `sinkTable`, regardless of whether
    * it's still usable against the source.
    */
  def stored(spark: SparkSession, sinkTable: String, name: String): Option[Long] = {
    val sink = Spark3Util.loadIcebergTable(spark, sinkTable)
    SnapshotUtil
      .currentAncestors(sink)
      .asScala
      .iterator
      .flatMap(s => Option(s.summary().get(propertyKey(name))))
      .nextOption()
      .map(_.toLong)
  }

  /** Snapshot-summary key `MaintainTables` puts on its rewrite commits: the
    * newest snapshot whose data the rewrite carries over. A rewrite adds no
    * rows, so reading incrementally from the rewrite snapshot sees exactly
    * the same new data as reading from that one.
    */
  val DataSnapshotKey: String = "pipeline.data-snapshot-id"

  /** The newest snapshot in `table`'s history that changed its data, for
    * `MaintainTables` to record on a rewrite: the current snapshot, unless
    * it's itself a rewrite, in which case the one that rewrite recorded.
    */
  def lastDataSnapshotId(table: Table): Option[Long] =
    SnapshotUtil
      .currentAncestors(table)
      .asScala
      .iterator
      .map { s =>
        if (s.operation() != "replace") Some(s.snapshotId())
        else Option(s.summary().get(DataSnapshotKey)).map(_.toLong)
      }
      .collectFirst { case Some(id) => id }

  /** Where to resume an incremental read of `source` for a sink that last
    * read up to `stored`: `stored` itself while it's still in the source's
    * history; else, if maintenance expired it, the rewrite commit that
    * recorded it as its data snapshot (nothing was added in between).
    */
  private def resumePoint(source: Table, stored: Long): Option[Long] =
    Option(source.currentSnapshot()).flatMap { current =>
      if (SnapshotUtil.isAncestorOf(source, current.snapshotId(), stored)) Some(stored)
      else
        SnapshotUtil
          .currentAncestors(source)
          .asScala
          .find(s => s.operation() == "replace" && Option(s.summary().get(DataSnapshotKey)).contains(stored.toString))
          .map(_.snapshotId())
    }

  /** The snapshot of `sourceTable` to read incrementally from for
    * `sinkTable`'s watermark `name`, or `None` on the sink's first run.
    *
    * Usually the snapshot recorded last time. When maintenance has expired
    * it, the rewrite that replaced it (see `DataSnapshotKey`). `None` when
    * neither is in `sourceTable`'s current history: after the source was
    * recreated with `createOrReplace()`, or when data was appended between
    * the recorded snapshot and a rewrite that's since been expired. An
    * incremental read from a missing snapshot fails with "is not a parent
    * ancestor of end snapshot", so the caller falls back to a full read —
    * which every job handles idempotently.
    */
  def get(spark: SparkSession, sinkTable: String, name: String, sourceTable: String): Option[Long] =
    stored(spark, sinkTable, name).flatMap { w =>
      val source = Spark3Util.loadIcebergTable(spark, sourceTable)
      val resume = resumePoint(source, w)
      resume match {
        case Some(r) if r != w =>
          println(s"Watermark: $sinkTable's '$name' ($w) was expired by maintenance — resuming from rewrite snapshot $r")
        case None =>
          println(
            s"Watermark: $sinkTable's '$name' ($w) is not in $sourceTable's current history " +
              "(source recreated or snapshot expired) — ignoring it and reading the full source"
          )
        case _ =>
      }
      resume
    }

  /** Runs `write`, recording each `name -> snapshotId` in the summary of
    * every Iceberg commit it makes, so the watermark lands atomically with
    * the data. `write` must commit at least once for the watermark to
    * advance (an Iceberg MERGE or append commits even when it writes no rows).
    */
  def withCommit[T](watermarks: (String, Long)*)(write: => T): T = {
    val properties = watermarks.map { case (name, id) => propertyKey(name) -> id.toString }.toMap.asJava
    CommitMetadata.withCommitProperties[T, RuntimeException](properties, () => write, classOf[RuntimeException])
  }
}
