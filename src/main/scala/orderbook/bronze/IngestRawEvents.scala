package orderbook.bronze

import orderbook.common.{OrderBookSchema, PolarisSpark, Watermark}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/** Ingestion job for `orderbook.bronze.raw_events`
  * (data_pipeline_plan.md, Phase 3). Reads the newline-delimited JSON files
  * Redpanda Connect lands under the S3 landing prefix
  * (`infra/redpanda-connect/landing.yaml`) and appends them to the bronze
  * table. Producers (e.g. `GenerateSyntheticEvents`) POST events over HTTP;
  * this job is the micro-batch step that moves landed files into Iceberg.
  *
  * Runs as a Structured Streaming query with `Trigger.AvailableNow`: it
  * processes every file that landed since the last run, commits, and stops,
  * so it's still a batch job from the Makefile's point of view. The
  * streaming checkpoint (`INGEST_CHECKPOINT`) records which files have been
  * read, so a run only reads new files rather than listing and re-reading
  * the whole landing prefix, and Iceberg's streaming sink records each
  * micro-batch's id in the commit, so a batch retried after a crash isn't
  * appended twice. Row-level dedupe by `(instrument, seq_no)` still happens
  * downstream in silver, for duplicates the producer itself sends.
  *
  * Schema-tolerant: every feed column is read as a string and cast to the
  * bronze type; a value that won't cast becomes null rather than failing the
  * append (see `OrderBookSchema.bronzeRawEvents`).
  *
  * Run with: sbt "runMain orderbook.bronze.IngestRawEvents [landing] [checkpoint]"
  *
  * `landing` defaults to the `STREAM_LANDING` env var, else `DefaultLanding`;
  * `checkpoint` to `INGEST_CHECKPOINT`, else `DefaultCheckpoint`.
  */
object IngestRawEvents {

  /** Where Redpanda Connect lands events (`infra/redpanda-connect/landing.yaml`). */
  val DefaultLanding: String = "s3://orderbook-warehouse/landing/raw_events"

  /** Where the streaming query keeps its progress (which files it has read).
    * `s3a://`, not `s3://`: Spark writes checkpoints through Hadoop's
    * FileContext API, whose S3A binding rejects any other scheme.
    */
  val DefaultCheckpoint: String = "s3a://orderbook-warehouse/_checkpoints/bronze_raw_events"

  /** The landed JSON, every feed column read as a string. Strings accept any
    * JSON value (Spark keeps a number's text), so parsing never fails on a
    * type mismatch; `conform` does the typing.
    */
  val landedSchema: StructType =
    StructType(OrderBookSchema.feedColumns.map(StructField(_, StringType, nullable = true)))

  /** Shapes landed rows (`landedSchema` plus a `_source_file` column) to the
    * bronze schema: casts each feed column, nulls what doesn't cast, and
    * stamps `_ingested_at`.
    */
  def conform(landed: DataFrame): DataFrame =
    OrderBookSchema.align(landed.withColumn("_ingested_at", current_timestamp()), OrderBookSchema.bronzeRawEvents)

  /** Ingests every file under `landing` not yet recorded in `checkpoint`. */
  def run(spark: SparkSession, catalog: String, landing: String, checkpoint: String): Unit = {
    val table = s"$catalog.bronze.raw_events"

    // Redpanda Connect only creates the prefix when it lands its first
    // file, and a streaming file source refuses a path that doesn't exist.
    val prefix = new Path(landing)
    val fs     = prefix.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(prefix)) {
      println(s"IngestRawEvents: no landed files under $landing yet — nothing to do (send events with make generate)")
      return
    }

    val before = Watermark.currentSnapshotId(spark, table)
    val query = spark.readStream
      .schema(landedSchema)
      .json(landing)
      .withColumn("_source_file", col("_metadata.file_path"))
      .transform(conform)
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint)
      .toTable(table)
    query.awaitTermination()

    val progress = query.recentProgress.filter(_.numInputRows > 0)
    if (progress.isEmpty)
      println(s"IngestRawEvents: no new files under $landing — nothing to do")
    else
      println(s"Appended ${progress.map(_.numInputRows).sum} events in ${progress.length} micro-batch(es) from $landing to $table")
    println(Watermark.describeCommit(spark, table, before))
  }

  def main(args: Array[String]): Unit = {
    val landing    = args.headOption.getOrElse(sys.env.getOrElse("STREAM_LANDING", DefaultLanding))
    val checkpoint = args.lift(1).getOrElse(sys.env.getOrElse("INGEST_CHECKPOINT", DefaultCheckpoint))

    val spark = PolarisSpark.session("orderbook-ingest-raw-events")
    run(spark, PolarisSpark.catalogName, landing, checkpoint)
    spark.stop()
  }
}
