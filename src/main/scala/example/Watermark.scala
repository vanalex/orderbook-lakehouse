package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.iceberg.spark.Spark3Util

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

  /** The snapshot id of the upstream table last recorded against `sinkTable`
    * under `name`, or `None` on the sink's first run.
    */
  def get(spark: SparkSession, sinkTable: String, name: String): Option[Long] =
    spark
      .sql(s"SHOW TBLPROPERTIES $sinkTable")
      .where(col("key") === propertyKey(name))
      .collect()
      .headOption
      .map(_.getString(1).toLong)

  /** Records `snapshotId` against `sinkTable` under `name`. */
  def set(spark: SparkSession, sinkTable: String, name: String, snapshotId: Long): Unit =
    spark.sql(s"ALTER TABLE $sinkTable SET TBLPROPERTIES ('${propertyKey(name)}' = '$snapshotId')")
}
