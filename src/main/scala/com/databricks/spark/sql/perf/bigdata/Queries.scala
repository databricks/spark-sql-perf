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

package com.databricks.spark.sql.perf.bigdata

import com.databricks.spark.sql.perf.Query

object Queries {
  val queries1to3 = Seq(
    Query("q1A",
      """
        |SELECT
        |  pageURL,
        |  pageRank
        |FROM rankings
        |WHERE
        |  pageRank > 1000
      """.stripMargin),

    Query("q1B",
      """
        |SELECT
        |  pageURL,
        |  pageRank
        |FROM rankings
        |WHERE
        |  pageRank > 100
      """.stripMargin),

    Query("q1C",
      """
        |SELECT
        |  pageURL,
        |  pageRank
        |FROM rankings
        |WHERE
        |  pageRank > 10
      """.stripMargin),

    Query("q2A",
      """
        |SELECT
        |  SUBSTR(sourceIP, 1, 8),
        |  SUM(adRevenue)
        |FROM uservisits
        |GROUP BY
        |  SUBSTR(sourceIP, 1, 8)
      """.stripMargin),

    Query("q2B",
      """
        |SELECT
        |  SUBSTR(sourceIP, 1, 10),
        |  SUM(adRevenue)
        |FROM uservisits
        |GROUP BY
        |  SUBSTR(sourceIP, 1, 10)
      """.stripMargin),

    Query("q2C",
      """
        |SELECT
        |  SUBSTR(sourceIP, 1, 12),
        |  SUM(adRevenue)
        |FROM uservisits
        |GROUP BY
        |  SUBSTR(sourceIP, 1, 12)
      """.stripMargin),

    Query("q3A",
      """
        |SELECT sourceIP, totalRevenue, avgPageRank
        |FROM
        |  (SELECT sourceIP,
        |          AVG(pageRank) as avgPageRank,
        |          SUM(adRevenue) as totalRevenue
        |    FROM Rankings AS R, UserVisits AS UV
        |    WHERE R.pageURL = UV.destURL
        |      AND UV.visitDate > "1980-01-01"
        |      AND UV.visitDate < "1980-04-01"
        |    GROUP BY UV.sourceIP) tmp
        |ORDER BY totalRevenue DESC LIMIT 1
      """.stripMargin),

    Query("q3B",
      """
        |SELECT sourceIP, totalRevenue, avgPageRank
        |FROM
        |  (SELECT sourceIP,
        |          AVG(pageRank) as avgPageRank,
        |          SUM(adRevenue) as totalRevenue
        |    FROM Rankings AS R, UserVisits AS UV
        |    WHERE R.pageURL = UV.destURL
        |      AND UV.visitDate > "1980-01-01"
        |      AND UV.visitDate < "1983-01-01"
        |    GROUP BY UV.sourceIP) tmp
        |ORDER BY totalRevenue DESC LIMIT 1
      """.stripMargin),

    Query("q3C",
      """
        |SELECT sourceIP, totalRevenue, avgPageRank
        |FROM
        |  (SELECT sourceIP,
        |          AVG(pageRank) as avgPageRank,
        |          SUM(adRevenue) as totalRevenue
        |    FROM Rankings AS R, UserVisits AS UV
        |    WHERE R.pageURL = UV.destURL
        |      AND UV.visitDate > "1980-01-01"
        |      AND UV.visitDate < "2010-01-01"
        |    GROUP BY UV.sourceIP) tmp
        |ORDER BY totalRevenue DESC LIMIT 1
      """.stripMargin)
  )
}
