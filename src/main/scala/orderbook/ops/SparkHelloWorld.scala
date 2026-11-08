package orderbook.ops

import org.apache.spark.sql.SparkSession

/** Minimal check that Spark itself runs on this machine (JVM, `--add-opens`
  * flags, local executor). It doesn't touch Polaris or SeaweedFS, so it
  * works without the Docker stack: if this fails, the problem is the local
  * Spark/Java setup, not the catalog or storage. For the full
  * Spark -> Polaris -> SeaweedFS path, use `SmokeTest`.
  *
  * Run with: sbt "runMain orderbook.ops.SparkHelloWorld"  (or `make spark-hello`)
  */
object SparkHelloWorld {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("orderbook-spark-hello-world")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    try {
      println(s"Hello from Spark ${spark.version} (Scala ${util.Properties.versionNumberString}, Java ${System.getProperty("java.version")})")

      val df = Seq(("BTC-USD", 1), ("ETH-USD", 2), ("BTC-USD", 3)).toDF("instrument", "n")
      df.createOrReplaceTempView("hello")
      spark.sql("SELECT instrument, count(*) AS events, sum(n) AS total FROM hello GROUP BY instrument ORDER BY instrument")
        .show()

      println("Spark hello world: OK")
    } finally {
      spark.stop()
    }
  }
}
