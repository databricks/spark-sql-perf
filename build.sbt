// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

name := "spark-sql-perf"

organization := "com.databricks"

scalaVersion := "2.10.4"

sparkPackageName := "databricks/spark-sql-perf"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkVersion := "1.6.0"

resolvers += "Apache Preview Repo" at "https://repository.apache.org/content/repositories/orgapachespark-1175/"

sparkComponents ++= Seq("sql", "hive")

initialCommands in console :=
  """
    |import org.apache.spark.sql._
    |import org.apache.spark.sql.functions._
    |import org.apache.spark.sql.types._
    |import org.apache.spark.sql.hive.test.TestHive
    |import TestHive.implicits
    |import TestHive.sql
    |
    |val sqlContext = TestHive
    |import sqlContext.implicits._
  """.stripMargin

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "com.twitter" %% "util-jvm" % "6.23.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

fork := true

// Your username to login to Databricks Cloud
dbcUsername := sys.env.getOrElse("DBC_USERNAME", sys.error("Please set DBC_USERNAME"))

// Your password (Can be set as an environment variable)
dbcPassword := sys.env.getOrElse("DBC_PASSWORD", sys.error("Please set DBC_PASSWORD"))

// The URL to the Databricks Cloud DB Api. Don't forget to set the port number to 34563!
dbcApiUrl := sys.env.getOrElse ("DBC_URL", sys.error("Please set DBC_URL"))

// Add any clusters that you would like to deploy your work to. e.g. "My Cluster"
// or run dbcExecuteCommand
dbcClusters += sys.env.getOrElse("DBC_USERNAME", sys.error("Please set DBC_USERNAME"))

dbcLibraryPath := s"/Users/${sys.env.getOrElse("DBC_USERNAME", sys.error("Please set DBC_USERNAME"))}/lib"

import ReleaseTransformations._

/** Push to the team directory instead of the user's homedir for releases. */
lazy val setupDbcRelease = ReleaseStep(
  action = { st: State =>
    val extracted = Project.extract(st)
    val newSettings = extracted.structure.allProjectRefs.map { ref =>
      dbcLibraryPath in ref := "/databricks/spark/sql/lib"
    }

    reapply(newSettings, st)
  }
)

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction := PgpKeys.publishSigned.value

pomExtra := (
      <url>https://github.com/databricks/spark-sql-perf</url>
      <scm>
        <url>git@github.com:databricks/spark-sql-perf.git</url>
        <connection>scm:git:git@github.com:databricks/spark-sql-perf.git</connection>
      </scm>
    )

bintrayReleaseOnPublish in ThisBuild := false

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setupDbcRelease,
  releaseStepTask(dbcUpload),
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)
