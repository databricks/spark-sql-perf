package com.databricks.spark.sql.perf

import org.apache.spark.sql.SQLContext

trait JoinPerformance extends Benchmark {
  // 1.5 mb, 1 file

  import ExecutionMode._

  val x = Table(
    "1milints",
    sqlContext.range(0, 1000000)
      .repartition(1))

  val joinTables = Seq(
    // 143.542mb, 10 files
    Table(
      "1bilints",
      sqlContext.range(0, 100000000)
        .repartition(10)),

    // 1.4348gb, 10 files
    Table(
      "1bilints",
      sqlContext.range(0, 1000000000)
      .repartition(10))
  )

  val sortMergeJoin = Variation("sortMergeJoin", Seq("on", "off")) {
    case "off" => sqlContext.setConf("spark.sql.planner.sortMergeJoin", "false")
    case "on" => sqlContext.setConf("spark.sql.planner.sortMergeJoin", "true")
  }

  val singleKeyJoins = Seq("1milints", "100milints", "1bilints").flatMap { table1 =>
    Seq("1milints", "100milints", "1bilints").flatMap { table2 =>
      Seq("JOIN", "RIGHT JOIN", "LEFT JOIN", "FULL OUTER JOIN").map { join =>
        Query(
          s"singleKey-$join-$table1-$table2",
          s"SELECT COUNT(*) FROM $table1 a $join $table2 b ON a.id = b.id",
          "equi-inner join a small table with a big table using a single key.",
          executionMode = CollectResults)
      }
    }
  }
}