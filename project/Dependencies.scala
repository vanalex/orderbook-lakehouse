import sbt._

object Dependencies {
  lazy val munit    = "org.scalameta" %% "munit"    % "0.7.29"
  lazy val requests = "com.lihaoyi"   %% "requests" % "0.9.0"
  lazy val upickle  = "com.lihaoyi"   %% "upickle"  % "4.1.0"

  private val sparkVersion   = "3.5.8"
  private val icebergVersion = "1.9.2"

  // Spark SQL, local mode (no external cluster needed for this demo).
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  // Iceberg's Spark runtime (bundles iceberg-core/api) + AWS bundle, which
  // provides S3FileIO for talking to MinIO. Matches the combo Polaris'
  // own getting-started docs use for --packages.
  lazy val icebergSparkRuntime =
    "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.13" % icebergVersion
  lazy val icebergAwsBundle =
    "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion

  // Hadoop's S3A FileSystem — Spark/Iceberg's S3FileIO (AWS SDK v2) handles
  // table data reads/writes, but Iceberg's Spark maintenance action for
  // orphan-file removal (`CALL ...system.remove_orphan_files`) lists the
  // table's storage location via Hadoop's `FileSystem` API instead, which
  // needs a registered filesystem for the `s3://` scheme. Version pinned to
  // match the hadoop-client Spark 3.5.8 already brings in transitively.
  lazy val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
}
