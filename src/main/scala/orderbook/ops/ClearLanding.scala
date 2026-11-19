package orderbook.ops

import orderbook.bronze.IngestRawEvents
import orderbook.common.PolarisSpark

import org.apache.hadoop.fs.Path

/** Resets ingest: deletes every file Redpanda Connect has landed under the
  * landing prefix (`STREAM_LANDING`, default `IngestRawEvents.DefaultLanding`)
  * and the ingest job's streaming checkpoint (`INGEST_CHECKPOINT`, default
  * `IngestRawEvents.DefaultCheckpoint`).
  *
  * `make setup` runs this together with recreating the tables. Recreating
  * bronze empties it, so the checkpoint's record of which files were already
  * ingested no longer matches the table, and files left in the landing
  * prefix would never be ingested again.
  *
  * Run with: sbt "runMain orderbook.ops.ClearLanding [landing] [checkpoint]"
  */
object ClearLanding {

  def main(args: Array[String]): Unit = {
    val landing    = args.headOption.getOrElse(sys.env.getOrElse("STREAM_LANDING", IngestRawEvents.DefaultLanding))
    val checkpoint = args.lift(1).getOrElse(sys.env.getOrElse("INGEST_CHECKPOINT", IngestRawEvents.DefaultCheckpoint))

    val spark = PolarisSpark.session("orderbook-clear-landing")
    val conf  = spark.sparkContext.hadoopConfiguration

    val prefix = new Path(landing)
    val fs     = prefix.getFileSystem(conf)
    if (!fs.exists(prefix)) {
      println(s"ClearLanding: $landing doesn't exist — nothing to clear")
    } else {
      val files = fs.listStatus(prefix).filter(_.isFile)
      files.foreach(f => fs.delete(f.getPath, false))
      println(s"ClearLanding: deleted ${files.length} landed file(s) under $landing")
    }

    val checkpointPath = new Path(checkpoint)
    val checkpointFs   = checkpointPath.getFileSystem(conf)
    if (checkpointFs.exists(checkpointPath)) {
      checkpointFs.delete(checkpointPath, true)
      println(s"ClearLanding: deleted the ingest checkpoint at $checkpoint")
    }

    spark.stop()
  }
}
