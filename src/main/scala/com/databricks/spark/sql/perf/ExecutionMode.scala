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

/**
 * Describes how a given Spark benchmark should be run (i.e. should the results be collected to
 * the driver or just computed on the executors.
 */
trait ExecutionMode extends Serializable
case object ExecutionMode {
  /** Benchmark run by collecting queries results  (e.g. rdd.collect()) */
  case object CollectResults extends ExecutionMode {
    override def toString: String = "collect"
  }

  /** Benchmark run by iterating through the queries results rows (e.g. rdd.foreach(row => Unit)) */
  case object ForeachResults extends ExecutionMode {
    override def toString: String = "foreach"
  }

  /** Benchmark run by saving the output of each query as a parquet file. */
  case class WriteParquet(location: String) extends ExecutionMode {
    override def toString: String = "saveToParquet"
  }

  /**
   * Benchmark run by calculating the sum of the hash value of all rows. This is used to check
   * query results do not change.
   */
  case object HashResults extends ExecutionMode {
    override def toString: String = "hash"
  }

  /** Results from Spark perf */
  case object SparkPerfResults extends ExecutionMode {
    override def toString: String = "sparkPerf"
  }
}