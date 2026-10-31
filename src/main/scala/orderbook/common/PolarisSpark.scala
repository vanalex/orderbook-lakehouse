package example

import org.apache.spark.sql.SparkSession

/** Builds a local SparkSession wired to the Polaris REST catalog + SeaweedFS,
  * mirroring the demo stack's defaults (see docker-compose.yml).
  *
  * Configuration is read from the environment (defaults match the demo
  * compose stack):
  *   - POLARIS_URL           (default http://localhost:8181)
  *   - POLARIS_REALM         (default default-realm)
  *   - POLARIS_CLIENT_ID     (default root)
  *   - POLARIS_CLIENT_SECRET (default s3cr3t)
  *   - POLARIS_CATALOG       (default orderbook)
  *   - S3_ENDPOINT           (default http://localhost:8333)
  *   - S3_ACCESS_KEY         (default admin)
  *   - S3_SECRET_KEY         (default password)
  *   - AWS_REGION            (default us-east-1)
  *   - SPARK_MASTER          (default local[*])
  */
object PolarisSpark {

  private def env(name: String, default: String): String =
    sys.env.getOrElse(name, default)

  /** Name Spark registers the catalog under, and the name of the catalog
    * as it exists in Polaris (the `warehouse` property below) — the demo
    * stack uses the same name for both, so one env var covers it.
    */
  val catalogName: String = env("POLARIS_CATALOG", "orderbook")

  def session(appName: String): SparkSession = {
    val polarisUrl    = env("POLARIS_URL", "http://localhost:8181")
    val realm         = env("POLARIS_REALM", "default-realm")
    val clientId      = env("POLARIS_CLIENT_ID", "root")
    val clientSecret  = env("POLARIS_CLIENT_SECRET", "s3cr3t")
    val s3Endpoint    = env("S3_ENDPOINT", "http://localhost:8333")
    val s3Access      = env("S3_ACCESS_KEY", "admin")
    val s3Secret      = env("S3_SECRET_KEY", "password")
    val awsRegion     = env("AWS_REGION", "us-east-1")

    val cat = s"spark.sql.catalog.$catalogName"

    SparkSession
      .builder()
      .appName(appName)
      .master(env("SPARK_MASTER", "local[*]"))
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      // Route the `orderbook` catalog through Iceberg's REST catalog client
      // at Polaris, authenticating via OAuth2 client-credentials (root).
      .config(cat, "org.apache.iceberg.spark.SparkCatalog")
      .config(s"$cat.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      .config(s"$cat.uri", s"$polarisUrl/api/catalog")
      .config(s"$cat.warehouse", catalogName)
      .config(s"$cat.credential", s"$clientId:$clientSecret")
      .config(s"$cat.scope", "PRINCIPAL_ROLE:ALL")
      .config(s"$cat.header.Polaris-Realm", realm)
      .config(s"$cat.token-refresh-enabled", "true")
      // Data files live in SeaweedFS; Polaris was set up with
      // SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION so it doesn't vend scoped
      // STS credentials — Spark authenticates to SeaweedFS directly instead.
      .config(s"$cat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config(s"$cat.s3.endpoint", s3Endpoint)
      .config(s"$cat.s3.path-style-access", "true")
      .config(s"$cat.s3.access-key-id", s3Access)
      .config(s"$cat.s3.secret-access-key", s3Secret)
      .config(s"$cat.client.region", awsRegion)
      // Iceberg's own table reads/writes go through S3FileIO above, but its
      // Spark maintenance actions (e.g. remove_orphan_files) list the
      // table's storage location via Hadoop's `FileSystem` API instead, so
      // that needs its own `s3://`-scheme filesystem registered — Hadoop's
      // S3A connector, pointed at the same S3 endpoint/credentials.
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.access.key", s3Access)
      .config("spark.hadoop.fs.s3a.secret.key", s3Secret)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3Endpoint.startsWith("https"))
      .getOrCreate()
  }
}
