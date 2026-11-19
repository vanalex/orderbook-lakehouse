package orderbook.ops

import orderbook.common.PolarisSpark

/** End-to-end smoke test for the Spark <-> Polaris <-> SeaweedFS wiring: creates
  * a real Iceberg table, writes rows, reads them back, then drops the table.
  *
  * Unlike `CreateNamespaces` (metadata only), this exercises the full data
  * path — Polaris' server-side commit bookkeeping and SeaweedFS's S3 API — which
  * is what actually needed the storage/network fixes documented in the
  * README ("Setup notes").
  *
  * Run with: sbt "runMain orderbook.ops.SmokeTest"
  */
object SmokeTest {

  def main(args: Array[String]): Unit = {
    val spark   = PolarisSpark.session("orderbook-smoke-test")
    val catalog = PolarisSpark.catalogName
    val table   = s"$catalog.bronze.smoke_test"

    // A fresh stack (docker compose down -v) has no namespaces until make setup;
    // create bronze here so the smoke test runs in any order. It's the same
    // namespace make setup creates, so leaving it behind changes nothing.
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.bronze")
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (id INT, name STRING) USING iceberg")
    spark.sql(s"INSERT INTO $table VALUES (1, 'abc'), (2, 'def')")

    val rows = spark.sql(s"SELECT * FROM $table").collect()
    spark.sql(s"DROP TABLE $table")
    spark.stop()

    val expected = Set((1, "abc"), (2, "def"))
    val actual   = rows.map(r => (r.getInt(0), r.getString(1))).toSet

    if (actual != expected) {
      throw new AssertionError(s"Expected $expected but got $actual")
    }

    println("SMOKE TEST OK")
  }
}
