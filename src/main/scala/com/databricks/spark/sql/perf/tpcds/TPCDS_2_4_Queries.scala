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

package com.databricks.spark.sql.perf.tpcds

import org.apache.commons.io.IOUtils

import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode, Query}

/**
 * This implements the official TPCDS v2.4 queries with only cosmetic modifications.
 */
trait Tpcds_2_4_Queries extends Benchmark {

  import ExecutionMode._

  val queryNames = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19",
    "q20", "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27",
    "q28", "q29", "q30", "q31", "q32", "q33", "q34", "q35", "q36", "q37",
    "q38", "q39a", "q39b", "q40", "q41", "q42", "q43", "q44", "q45", "q46", "q47",
    "q48", "q49", "q50", "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58",
    "q59", "q60", "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69",
    "q70", "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79",
    "q80", "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89",
    "q90", "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99",
    "ss_max"
  )

  val tpcds2_4Queries = queryNames.map { queryName =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpcds_2_4/$queryName.sql"))
    Query(queryName + "-v2.4", queryContent, description = "TPCDS 2.4 Query",
      executionMode = CollectResults)
  }

  val tpcds2_4QueriesMap = tpcds2_4Queries.map(q => q.name.split("-").get(0) -> q).toMap
}
