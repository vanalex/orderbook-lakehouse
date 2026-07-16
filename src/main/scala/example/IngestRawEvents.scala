package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

/** Batch ingestion job for `orderbook.bronze.raw_events`
  * (data_pipeline_plan.md, Phase 3). Reads raw feed files from a source
  * path — whatever format holds the historical replay data or synthetic
  * generator output — and appends them to the bronze table.
  *
  * Schema-tolerant: only columns known to `OrderBookSchema.bronzeRawEvents`
  * are kept, missing ones are filled with null, and values are cast to the
  * bronze types, so the source doesn't need to match column order or types
  * exactly. Idempotent by design in the sense the plan intends: appends
  * never dedupe here — dedup by `(instrument, seq_no)` happens downstream
  * in silver (Phase 4), not on write.
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
    * and source-specific typing (e.g. CSV's all-string columns).
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

  def main(args: Array[String]): Unit = {
    val path   = args.headOption.getOrElse(sys.env.getOrElse("SOURCE_PATH", "data/raw_events"))
    val format = args.lift(1).getOrElse(sys.env.getOrElse("SOURCE_FORMAT", "json"))

    val spark   = PolarisSpark.session("orderbook-ingest-raw-events")
    val catalog = PolarisSpark.catalogName
    val table   = s"$catalog.bronze.raw_events"

    val reader = spark.read.format(format)
    val raw    = (if (format == "csv") reader.option("header", "true") else reader).load(path)

    val events = conform(raw)
    events.writeTo(table).append()

    println(s"Appended ${events.count()} events from $path ($format) to $table")
    spark.stop()
  }
}
