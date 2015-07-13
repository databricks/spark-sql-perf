package com.databricks.spark.sql.perf

import org.apache.spark.sql.SQLContext

class JoinPerformance(sqlContext: SQLContext) extends Benchmark(sqlContext) {
  def buildTables() = {
    // 1.5 mb, 1 file
    sqlContext.range(0, 1000000)
      .repartition(1)
      .write.mode("ignore")
      .saveAsTable("1milints")

    // 143.542mb, 10 files
    sqlContext.range(0, 100000000)
      .repartition(10)
      .write.mode("ignore")
      .saveAsTable("100milints")

    // 1.4348gb, 10 files
    sqlContext.range(0, 1000000000)
      .repartition(10)
      .write.mode("ignore")
      .saveAsTable("1bilints")
  }

  val singleKeyJoins = Seq("1milints", "100milints", "1bilints").flatMap { table1 =>
    Seq("1milints", "100milints", "1bilints").flatMap { table2 =>
      Seq("JOIN", "RIGHT JOIN", "LEFT JOIN", "FULL OUTER JOIN").map { join =>
        Query(
          s"singleKey-$join-$table1-$table2",
          s"SELECT COUNT(*) FROM $table1 a $join $table2 b ON a.id = b.id",
          "equi-inner join a small table with a big table using a single key.",
          collectResults = true)
      }
    }
  }.filterNot(_.name contains "FULL OUTER JOIN-1milints-1bilints")

  val complexInput =
    Seq("1milints", "100milints", "1bilints").map { table =>
      Query(
        "aggregation-complex-input",
        s"SELECT SUM(id + id + id + id + id + id + id + id + id + id) FROM $table",
        "Sum of 9 columns added together",
        collectResults = true)
    }

  val aggregates =
    Seq("1milints", "100milints", "1bilints").flatMap { table =>
      Seq("SUM", "AVG", "COUNT", "STDDEV").map { agg =>
        Query(
          s"single-aggregate-$agg",
          s"SELECT $agg(id) FROM $table",
          "aggregation of a single column",
          collectResults = true)
      }
    }
}