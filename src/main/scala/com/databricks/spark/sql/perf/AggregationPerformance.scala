package com.databricks.spark.sql.perf

import org.apache.spark.sql.{Row, SQLContext}

trait AggregationPerformance extends Benchmark {

  import sqlContext.implicits._
  import ExecutionMode._


  val sizes = (1 to 6).map(math.pow(10, _).toInt).toSeq

  val variousCardinality = sizes.map { size =>
    Table(s"ints$size",
      sparkContext.parallelize(1 to size).flatMap { group =>
        (1 to 10000).map(i => (group, i))
      }.toDF("a", "b"))
  }

  val lowCardinality = sizes.map { size =>
    val fullSize = size * 10000L
    Table(
      s"twoGroups$fullSize",
      sqlContext.range(0, fullSize).select($"id" % 2 as 'a, $"id" as 'b))
  }

  val newAggreation = Variation("aggregationType", Seq("new", "old")) {
    case "old" => sqlContext.setConf("spark.sql.useAggregate2", "false")
    case "new" => sqlContext.setConf("spark.sql.useAggregate2", "true")
  }

  val varyNumGroupsAvg: Seq[Query] = variousCardinality.map(_.name).map { table =>
    Query(
      s"avg-$table",
      s"SELECT AVG(b) FROM $table GROUP BY a",
      "an average with a varying number of groups",
      executionMode = ForeachResults)
  }

  val twoGroupsAvg: Seq[Query] = lowCardinality.map(_.name).map { table =>
    Query(
      s"avg-$table",
      s"SELECT AVG(b) FROM $table GROUP BY a",
      "an average on an int column with only two groups",
      executionMode = ForeachResults)
  }

  val complexInput =
    Seq("1milints", "100milints", "1bilints").map { table =>
      Query(
        s"aggregation-complex-input-$table",
        s"SELECT SUM(id + id + id + id + id + id + id + id + id + id) FROM $table",
        "Sum of 9 columns added together",
        executionMode = CollectResults)
    }

  val aggregates =
    Seq("1milints", "100milints", "1bilints").flatMap { table =>
      Seq("SUM", "AVG", "COUNT", "STDDEV").map { agg =>
        Query(
          s"single-aggregate-$agg-$table",
          s"SELECT $agg(id) FROM $table",
          "aggregation of a single column",
          executionMode = CollectResults)
      }
    }
}