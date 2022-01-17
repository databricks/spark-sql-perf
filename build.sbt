// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

name := "spark-sql-perf"

organization := "com.databricks"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.12.10")

sparkPackageName := "databricks/spark-sql-perf"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkVersion := "3.2.0"

sparkComponents ++= Seq("sql", "hive", "mllib")


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

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

libraryDependencies += "com.twitter" %% "util-jvm" % "6.45.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.23"

fork := true

// Your username to login to Databricks Cloud
dbcUsername := sys.env.getOrElse("DBC_USERNAME", "")

// Your password (Can be set as an environment variable)
dbcPassword := sys.env.getOrElse("DBC_PASSWORD", "")

// The URL to the Databricks Cloud DB Api. Don't forget to set the port number to 34563!
dbcApiUrl := sys.env.getOrElse ("DBC_URL", sys.error("Please set DBC_URL"))

// Add any clusters that you would like to deploy your work to. e.g. "My Cluster"
// or run dbcExecuteCommand
dbcClusters += sys.env.getOrElse("DBC_USERNAME", "")

dbcLibraryPath := s"/Users/${sys.env.getOrElse("DBC_USERNAME", "")}/lib"

val runBenchmark = inputKey[Unit]("runs a benchmark")

runBenchmark := {
  import complete.DefaultParsers._
  val args = spaceDelimited("[args]").parsed
  val scalaRun = (runner in run).value
  val classpath = (fullClasspath in Compile).value
  scalaRun.run("com.databricks.spark.sql.perf.RunBenchmark", classpath.map(_.data), args,
    streams.value.log)
}


val runMLBenchmark = inputKey[Unit]("runs an ML benchmark")

runMLBenchmark := {
  import complete.DefaultParsers._
  val args = spaceDelimited("[args]").parsed
  val scalaRun = (runner in run).value
  val classpath = (fullClasspath in Compile).value
  scalaRun.run("com.databricks.spark.sql.perf.mllib.MLLib", classpath.map(_.data), args,
    streams.value.log)
}


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
      <developers>
        <developer>
          <id>marmbrus</id>
          <name>Michael Armbrust</name>
          <url>https://github.com/marmbrus</url>
        </developer>
        <developer>
          <id>yhuai</id>
          <name>Yin Huai</name>
          <url>https://github.com/yhuai</url>
        </developer>
        <developer>
          <id>nongli</id>
          <name>Nong Li</name>
          <url>https://github.com/nongli</url>
        </developer>
        <developer>
          <id>andrewor14</id>
          <name>Andrew Or</name>
          <url>https://github.com/andrewor14</url>
        </developer>
        <developer>
          <id>davies</id>
          <name>Davies Liu</name>
          <url>https://github.com/davies</url>
        </developer>
      </developers>
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
