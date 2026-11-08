package orderbook.ops

import orderbook.bronze.IngestRawEvents
import orderbook.common.PolarisSpark

import org.apache.hadoop.fs.Path

/** Deletes every file Redpanda Connect has landed under the S3 landing prefix
  * (`STREAM_LANDING`, default `IngestRawEvents.DefaultLanding`).
  *
  * `make setup` runs this together with recreating the tables: recreating
  * `bronze.ingested_files` forgets which landed files were already loaded,
  * so files left in the landing prefix would be ingested again.
  *
  * Run with: sbt "runMain orderbook.ops.ClearLanding [path]"
  */
object ClearLanding {

  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse(sys.env.getOrElse("STREAM_LANDING", IngestRawEvents.DefaultLanding))

    val spark  = PolarisSpark.session("orderbook-clear-landing")
    val prefix = new Path(path)
    val fs     = prefix.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (!fs.exists(prefix)) {
      println(s"ClearLanding: $path doesn't exist — nothing to clear")
    } else {
      val files = fs.listStatus(prefix).filter(_.isFile)
      files.foreach(f => fs.delete(f.getPath, false))
      println(s"ClearLanding: deleted ${files.length} landed file(s) under $path")
    }

    spark.stop()
  }
}
