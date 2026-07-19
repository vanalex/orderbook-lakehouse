import Dependencies._

ThisBuild / scalaVersion     := "2.13.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

// Spark needs reflective access to JDK internals that Java 17+ closes by
// default. `fork` runs `run`/`test` in their own JVM so these flags apply
// (they'd otherwise have to be set on the sbt launcher's JVM itself).
lazy val javaModuleOpens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "orderbook-lakehouse",
    libraryDependencies ++= Seq(
      requests,
      upickle,
      sparkSql,
      icebergSparkRuntime,
      icebergAwsBundle,
      hadoopAws,
      munit % Test
    ),
    fork := true,
    javaOptions ++= javaModuleOpens
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
