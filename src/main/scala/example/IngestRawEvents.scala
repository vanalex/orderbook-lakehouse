package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, lit}

/** Batch ingestion job for `orderbook.bronze.raw_events`
  * (data_pipeline_plan.md, Phase 3). Reads raw feed files from a source
  * path — whatever format holds the historical replay data or synthetic
  * generator output — and appends them to the bronze table.
  *
  * Schema-tolerant: only columns known to `OrderBookSchema.bronzeRawEvents`
  * are kept, missing ones are filled with null, and values are cast to the
  * bronze types, so the source doesn't need to match column order or types
  * exactly.
  *
  * Tracks which source files it's already ingested, in
  * `orderbook.bronze.ingested_files` (one row per file path, as Spark's
  * `input_file_name()` reports it): a run only reads/appends rows from
  * files not already recorded there, so re-running against a landing
  * directory that keeps growing (e.g. `GenerateSyntheticEvents` appending
  * another batch) doesn't re-append files already ingested. This is
  * file-level idempotency, distinct from and in addition to the
  * row-level dedupe by `(instrument, seq_no)` that happens downstream in
  * silver (Phase 4) — that still exists as the correctness backstop for
  * duplicate rows *within* a file or across sources, the same way
  * `Watermark` is a pure efficiency win on top of silver's own
  * insert-only merge (Phase 6).
  *
  * Run with: sbt "runMain example.IngestRawEvents <path> [format]"
  * e.g.:     sbt "runMain example.IngestRawEvents data/raw_events json"
  *
  * `format` is one of Spark's built-in reader formats (`json`, `csv`,
  * `parquet`); defaults to `json`. `path`/`format` can also come from the
  * `SOURCE_PATH`/`SOURCE_FORMAT` env vars.
  */
object IngestRawEvents {

  /** Aligns `raw` to `OrderBookSchema.bronzeRawEvents`: keeps only the known
    * columns (by name), adds any missing ones as null, and casts everything
    * to the bronze types — tolerant of extra/missing columns, column order,
    * and source-specific typing (e.g. CSV's all-string columns). Any other
    * columns present (e.g. a `_source_file` tracking column) are dropped by
    * the final `select`.
    */
  def conform(raw: DataFrame): DataFrame = {
    val schema  = OrderBookSchema.bronzeRawEvents
    val present = raw.columns.toSet

    val withAllColumns = schema.fields.foldLeft(raw) { (df, field) =>
      if (present.contains(field.name)) df
      else df.withColumn(field.name, lit(null).cast(field.dataType))
    }

    withAllColumns.select(schema.fields.map(f => col(f.name).cast(f.dataType).as(f.name)): _*)
  }

  /** Drops rows whose `sourceFileCol` value already appears in
    * `ingestedFiles`' `path` column, so only rows from files not yet
    * ingested survive.
    */
  def newFilesOnly(withSourceFile: DataFrame, ingestedFiles: DataFrame, sourceFileCol: String): DataFrame =
    withSourceFile.join(ingestedFiles, withSourceFile(sourceFileCol) === ingestedFiles("path"), "left_anti")

  def main(args: Array[String]): Unit = {
    val path   = args.headOption.getOrElse(sys.env.getOrElse("SOURCE_PATH", "data/raw_events"))
    val format = args.lift(1).getOrElse(sys.env.getOrElse("SOURCE_FORMAT", "json"))

    val spark   = PolarisSpark.session("orderbook-ingest-raw-events")
    val catalog = PolarisSpark.catalogName
    val table      = s"$catalog.bronze.raw_events"
    val filesTable = s"$catalog.bronze.ingested_files"

    val reader = spark.read.format(format)
    val raw = (if (format == "csv") reader.option("header", "true") else reader)
      .load(path)
      .withColumn("_source_file", input_file_name())

    val ingestedFiles = spark.table(filesTable).select(col("path"))
    val newRaw        = newFilesOnly(raw, ingestedFiles, "_source_file").cache()

    val newFiles = newRaw.select(col("_source_file").as("path")).distinct().cache()
    val fileCount = newFiles.count()

    if (fileCount == 0L) {
      println(s"IngestRawEvents: no new files under $path ($format) — nothing to do")
    } else {
      val events = conform(newRaw)
      events.writeTo(table).append()
      println(s"Appended ${events.count()} events from $fileCount new file(s) under $path ($format) to $table")

      newFiles.withColumn("ingested_at", current_timestamp()).writeTo(filesTable).append()
      println(s"Recorded $fileCount file(s) in $filesTable")
    }

    spark.stop()
  }
}
