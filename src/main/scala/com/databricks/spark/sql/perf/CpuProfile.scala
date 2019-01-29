/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import java.io.{FileOutputStream, File}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions._

import scala.language.reflectiveCalls
import scala.sys.process._

import org.apache.hadoop.fs.{FileSystem, Path}

import com.twitter.jvm.CpuProfile

/**
 * A collection of utilities for parsing stacktraces that have been recorded in JSON and generating visualizations
 * on where time is being spent.
 */
package object cpu {

  // Placeholder for DBFS.
  type FS = {
    def cp(from: String, to: String, recurse: Boolean): Boolean
    def rm(dir: String, recurse: Boolean): Boolean
  }

  private val resultsLocation = "/spark/sql/cpu"

  lazy val pprof = {
    run(
      "sudo apt-get install -y graphviz",
      "cp /dbfs/home/michael/pprof ./",
      "chmod 755 pprof")

    "./pprof"
  }

  def getCpuLocation(timestamp: Long) = s"$resultsLocation/timestamp=$timestamp"

  def collectLogs(sqlContext: SQLContext, fs: FS, timestamp: Long): String = {
    import sqlContext.implicits._

    def sc = sqlContext.sparkContext

    def copyLogFiles() = {
      val path = "pwd".!!.trim
      val hostname = "hostname".!!.trim

      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      fs.copyFromLocalFile(new Path(s"$path/logs/cpu.json"), new Path(s"$resultsLocation/timestamp=$timestamp/$hostname"))
    }

    fs.rm(getCpuLocation(timestamp), true)

    copyLogFiles()
    sc.parallelize((1 to 100)).foreach { i => copyLogFiles() }
    getCpuLocation(timestamp)
  }

  def run(cmds: String*) = {
    val output = new StringBuilder

    def append(line: String): Unit = output.synchronized {
      println(line)
      output.append(line)
      output.append("\n")
    }

    val processLogger = ProcessLogger(append, append)

    val exitCode = Seq("/bin/bash", "-c", cmds.mkString(" && ")) ! processLogger

    (exitCode, output.toString())
  }

  class Profile(private val sqlContext: SQLContext, cpuLogs: DataFrame) {
    import sqlContext.implicits._

    def hosts = cpuLogs.select($"tags.hostName").distinct.collect().map(_.getString(0))

    def buildGraph(fs: FS) = {
      val stackLine = """(.*)\.([^\(]+)\(([^:]+)(:{0,1}\d*)\)""".r
      def toStackElement(s: String) = s match {
        case stackLine(cls, method, file, "") => new StackTraceElement(cls, method, file, 0)
        case stackLine(cls, method, file, line) => new StackTraceElement(cls, method, file, line.stripPrefix(":").toInt)
      }

      val counts = cpuLogs.groupBy($"stack").agg(count($"*")).collect().flatMap {
        case Row(stackLines: Array[String], count: Long) => stackLines.toSeq.map(toStackElement) -> count :: Nil
        case other => println(s"Failed to parse $other"); Nil
      }.toMap
      val profile = new com.twitter.jvm.CpuProfile(counts, com.twitter.util.Duration.fromSeconds(10), cpuLogs.count().toInt, 0)

      val outfile = File.createTempFile("cpu", "profile")
      val svgFile = File.createTempFile("cpu", "svg")

      profile.writeGoogleProfile(new FileOutputStream(outfile))

      println(run(
        "cp /dbfs/home/michael/pprof ./",
        "chmod 755 pprof",
        s"$pprof --svg ${outfile.getCanonicalPath} > ${svgFile.getCanonicalPath}"))

      val timestamp = System.currentTimeMillis()
      fs.cp(s"file://$svgFile", s"/FileStore/cpu.profiles/$timestamp.svg", false)
      s"""<a href="https://dogfood.staging.cloud.databricks.com/files/cpu.profiles/$timestamp.svg"/>CPU Usage Visualization</a>"""
    }
  }
}
