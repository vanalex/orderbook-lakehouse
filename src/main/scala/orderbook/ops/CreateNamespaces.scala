package orderbook.ops

import orderbook.common.PolarisSpark

import org.apache.spark.sql.SparkSession

/** Sanity-check job for the Spark <-> Polaris wiring: creates the
  * bronze/silver/gold namespaces the medallion pipeline will use, then
  * lists what's in the catalog to confirm connectivity end to end.
  *
  * Run with: sbt "runMain orderbook.ops.CreateNamespaces"
  */
object CreateNamespaces {

  val Namespaces: Seq[String] = Seq("bronze", "silver", "gold")

  def create(spark: SparkSession, catalog: String): Unit =
    Namespaces.foreach(ns => spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$ns"))

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-namespaces")
    val catalog = PolarisSpark.catalogName

    create(spark, catalog)

    println(s"Namespaces in '$catalog':")
    spark.sql(s"SHOW NAMESPACES IN $catalog").show(truncate = false)

    spark.stop()
  }
}
