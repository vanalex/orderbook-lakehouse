package example

/** Sanity-check job for the Spark <-> Polaris wiring: creates the
  * bronze/silver/gold namespaces the medallion pipeline will use, then
  * lists what's in the catalog to confirm connectivity end to end.
  *
  * Run with: sbt "runMain example.CreateNamespaces"
  */
object CreateNamespaces {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-create-namespaces")
    val catalog = PolarisSpark.catalogName

    Seq("bronze", "silver", "gold").foreach { ns =>
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$ns")
    }

    println(s"Namespaces in '$catalog':")
    spark.sql(s"SHOW NAMESPACES IN $catalog").show(truncate = false)

    spark.stop()
  }
}
