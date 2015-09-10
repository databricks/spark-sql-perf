package com.databricks.spark.sql.perf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait JoinPerformance extends Benchmark {
  // 1.5 mb, 1 file

  import ExecutionMode._
  import sqlContext.implicits._

  private val table = sqlContext.table _

  val x = Table(
    "1milints",
    sqlContext.range(0, 1000000)
      .repartition(1))

  val joinTables = Seq(
    // 143.542mb, 10 files
    Table(
      "100milints",
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

  val varyDataSize = Seq(1, 128, 256, 512, 1024).map { dataSize =>
    val intsWithData = table("100milints").select($"id", lit("*" * dataSize).as(s"data$dataSize"))
    new Query(
      s"join - datasize: $dataSize",
      intsWithData.as("a").join(intsWithData.as("b"), $"a.id" === $"b.id"))
  }

  val varyKeyType = Seq(StringType, IntegerType, LongType, DoubleType).map { keyType =>
    val convertedInts = table("100milints").select($"id".cast(keyType).as("id"))
    new Query(
      s"join - keytype: $keyType",
      convertedInts.as("a").join(convertedInts.as("b"), $"a.id" === $"b.id"))
  }

  val varyNumMatches = Seq(1, 2, 4, 8, 16).map { numCopies =>
    val ints = table("100milints")
    val copiedInts = Seq.fill(numCopies)(ints).reduce(_ unionAll _)
    new Query(
      s"join - numMatches: $numCopies",
      copiedInts.as("a").join(ints.as("b"), $"a.id" === $"b.id"))
  }
}
