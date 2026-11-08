package orderbook.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.util.SnapshotUtil

/** Tracks "last upstream snapshot id processed" as an Iceberg table property
  * on the sink table, so a job can incrementally scan its source via
  * Iceberg's `start-snapshot-id`/`end-snapshot-id` read instead of
  * rescanning full history every run (Phase 6 of data_pipeline_plan.md).
  */
object Watermark {

  private def propertyKey(name: String): String = s"pipeline.watermark.$name"

  /** Current snapshot id of `table`, or `None` if it has no snapshots yet
    * (freshly created, never written to).
    */
  def currentSnapshotId(spark: SparkSession, table: String): Option[Long] =
    Option(Spark3Util.loadIcebergTable(spark, table).currentSnapshot()).map(_.snapshotId())

  /** The snapshot id of `sourceTable` last recorded against `sinkTable`
    * under `name`, or `None` on the sink's first run.
    *
    * Also `None` when the stored id is no longer in `sourceTable`'s current
    * history, e.g. after the source was recreated with `createOrReplace()`
    * (Iceberg's replace keeps the sink's properties, so the old watermark
    * survives). An incremental read from such a snapshot fails with "is not a
    * parent ancestor of end snapshot", so the caller falls back to a full read.
    */
  def get(spark: SparkSession, sinkTable: String, name: String, sourceTable: String): Option[Long] = {
    val stored = spark
      .sql(s"SHOW TBLPROPERTIES $sinkTable")
      .where(col("key") === propertyKey(name))
      .collect()
      .headOption
      .map(_.getString(1).toLong)

    stored.filter { w =>
      val source  = Spark3Util.loadIcebergTable(spark, sourceTable)
      val current = Option(source.currentSnapshot()).map(_.snapshotId())
      val valid   = current.exists(c => SnapshotUtil.isAncestorOf(source, c, w))
      if (!valid)
        println(
          s"Watermark: $sinkTable's '$name' ($w) is not in $sourceTable's current history " +
            "(source was recreated?) — ignoring it and reading the full source"
        )
      valid
    }
  }

  /** Records `snapshotId` against `sinkTable` under `name`. */
  def set(spark: SparkSession, sinkTable: String, name: String, snapshotId: Long): Unit =
    spark.sql(s"ALTER TABLE $sinkTable SET TBLPROPERTIES ('${propertyKey(name)}' = '$snapshotId')")
}
