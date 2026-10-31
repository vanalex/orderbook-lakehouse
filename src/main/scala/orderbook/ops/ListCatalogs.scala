package example

/** Lists the catalogs registered in Polaris via its management API.
  *
  * Fetches an OAuth token as the root principal, then GETs
  * `/api/management/v1/catalogs` and prints each catalog.
  *
  * Configuration is read from the environment (defaults match the demo
  * compose stack):
  *   - POLARIS_URL           (default http://localhost:8181)
  *   - POLARIS_REALM         (default default-realm)
  *   - POLARIS_CLIENT_ID     (default root)
  *   - POLARIS_CLIENT_SECRET (default s3cr3t)
  *
  * Run with: sbt "runMain example.ListCatalogs"
  */
object ListCatalogs {

  private val base   = sys.env.getOrElse("POLARIS_URL", "http://localhost:8181")
  private val realm  = sys.env.getOrElse("POLARIS_REALM", "default-realm")
  private val id     = sys.env.getOrElse("POLARIS_CLIENT_ID", "root")
  private val secret = sys.env.getOrElse("POLARIS_CLIENT_SECRET", "s3cr3t")

  def main(args: Array[String]): Unit = {
    val token    = fetchToken()
    val catalogs = listCatalogs(token)

    if (catalogs.isEmpty) {
      println("No catalogs found.")
    } else {
      println(s"Found ${catalogs.size} catalog(s):")
      catalogs.foreach { c =>
        val name = c("name").str
        val typ  = c("type").str
        val loc  = c("properties").obj.get("default-base-location").map(_.str).getOrElse("-")
        println(s"  - $name  [$typ]  -> $loc")
      }
    }
  }

  /** Obtain an access token via the OAuth client-credentials flow. */
  private def fetchToken(): String = {
    val resp = requests.post(
      s"$base/api/catalog/v1/oauth/tokens",
      headers = Map("Polaris-Realm" -> realm),
      data = Map(
        "grant_type"    -> "client_credentials",
        "client_id"     -> id,
        "client_secret" -> secret,
        "scope"         -> "PRINCIPAL_ROLE:ALL"
      )
    )
    ujson.read(resp.text())("access_token").str
  }

  /** GET the list of catalogs from the management API. */
  private def listCatalogs(token: String): Seq[ujson.Value] = {
    val resp = requests.get(
      s"$base/api/management/v1/catalogs",
      headers = Map(
        "Authorization" -> s"Bearer $token",
        "Polaris-Realm" -> realm
      )
    )
    ujson.read(resp.text())("catalogs").arr.toSeq
  }
}
