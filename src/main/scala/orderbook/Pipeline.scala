package orderbook

import orderbook.bronze.IngestRawEvents
import orderbook.common.PolarisSpark
import orderbook.gold.BuildGoldAggregates
import orderbook.silver.BuildSilverEvents

/** Runs the whole incremental pipeline — ingest, silver, gold — in one
  * SparkSession, instead of starting a JVM and a Spark session per stage.
  * Each stage is the same `run` its own job calls, so they can still be run
  * one at a time (`make ingest`, `make silver`, `make gold`).
  *
  * Run with: sbt "runMain orderbook.Pipeline"
  * Uses the same environment variables as `IngestRawEvents`
  * (`STREAM_LANDING`, `INGEST_CHECKPOINT`).
  */
object Pipeline {

  def main(args: Array[String]): Unit = {
    val landing    = sys.env.getOrElse("STREAM_LANDING", IngestRawEvents.DefaultLanding)
    val checkpoint = sys.env.getOrElse("INGEST_CHECKPOINT", IngestRawEvents.DefaultCheckpoint)

    val spark   = PolarisSpark.session("orderbook-pipeline")
    val catalog = PolarisSpark.catalogName

    println("== ingest ==")
    IngestRawEvents.run(spark, catalog, landing, checkpoint)
    println("== silver ==")
    BuildSilverEvents.run(spark, catalog)
    println("== gold ==")
    BuildGoldAggregates.run(spark, catalog)

    spark.stop()
  }
}
