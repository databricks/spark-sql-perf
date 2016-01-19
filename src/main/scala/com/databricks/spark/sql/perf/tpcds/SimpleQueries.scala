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

import com.databricks.spark.sql.perf.{ExecutionMode, Benchmark}

trait SimpleQueries extends Benchmark {

  import ExecutionMode._

   val targetedPerfQueries = Seq(
     // Query to measure scan performance.
     ("stores-sales-scan",
       """
         |select * from store_sales where ss_item_sk = 1
       """.stripMargin),
     ("fact-fact-join",
       """
         | select count(*) from store_sales
         | join store_returns
         | on store_sales.ss_item_sk = store_returns.sr_item_sk
         | and store_sales.ss_ticket_number = store_returns.sr_ticket_number
       """.stripMargin)
   ).map { case (name, sqlText) =>
     Query(name = name, sqlText = sqlText, description = "", executionMode = ForeachResults)
   }

   val q7Derived = Seq(
     ("q7-simpleScan",
       """
         |select
         |  ss_quantity,
         |  ss_list_price,
         |  ss_coupon_amt,
         |  ss_coupon_amt,
         |  ss_cdemo_sk,
         |  ss_item_sk,
         |  ss_promo_sk,
         |  ss_sold_date_sk
         |from store_sales
         |where
         |  ss_sold_date_sk between 2450815 and 2451179
       """.stripMargin),

     ("q7-twoMapJoins", """
                          |select
                          |  i_item_id,
                          |  ss_quantity,
                          |  ss_list_price,
                          |  ss_coupon_amt,
                          |  ss_sales_price,
                          |  ss_promo_sk,
                          |  ss_sold_date_sk
                          |from
                          |  store_sales
                          |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
                          |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                          |where
                          |  cd_gender = 'F'
                          |  and cd_marital_status = 'W'
                          |  and cd_education_status = 'Primary'
                          |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
                        """.stripMargin),

     ("q7-fourMapJoins", """
                           |select
                           |  i_item_id,
                           |  ss_quantity,
                           |  ss_list_price,
                           |  ss_coupon_amt,
                           |  ss_sales_price
                           |from
                           |  store_sales
                           |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
                           |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                           |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
                           |  join date_dim on (ss_sold_date_sk = d_date_sk)
                           |where
                           |  cd_gender = 'F'
                           |  and cd_marital_status = 'W'
                           |  and cd_education_status = 'Primary'
                           |  and (p_channel_email = 'N'
                           |    or p_channel_event = 'N')
                           |  and d_year = 1998
                           |  -- and ss_date between '1998-01-01' and '1998-12-31'
                           |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
                         """.stripMargin),

     ("q7-noOrderBy", """
                        |select
                        |  i_item_id,
                        |  avg(ss_quantity) agg1,
                        |  avg(ss_list_price) agg2,
                        |  avg(ss_coupon_amt) agg3,
                        |  avg(ss_sales_price) agg4
                        |from
                        |  store_sales
                        |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
                        |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                        |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
                        |  join date_dim on (ss_sold_date_sk = d_date_sk)
                        |where
                        |  cd_gender = 'F'
                        |  and cd_marital_status = 'W'
                        |  and cd_education_status = 'Primary'
                        |  and (p_channel_email = 'N'
                        |    or p_channel_event = 'N')
                        |  and d_year = 1998
                        |  -- and ss_date between '1998-01-01' and '1998-12-31'
                        |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
                        |group by
                        |  i_item_id
                      """.stripMargin),

     ("q7", """
              |-- start query 1 in stream 0 using template query7.tpl
              |select
              |  i_item_id,
              |  avg(ss_quantity) agg1,
              |  avg(ss_list_price) agg2,
              |  avg(ss_coupon_amt) agg3,
              |  avg(ss_sales_price) agg4
              |from
              |  store_sales
              |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
              |  join date_dim on (ss_sold_date_sk = d_date_sk)
              |where
              |  cd_gender = 'F'
              |  and cd_marital_status = 'W'
              |  and cd_education_status = 'Primary'
              |  and (p_channel_email = 'N'
              |    or p_channel_event = 'N')
              |  and d_year = 1998
              |  -- and ss_date between '1998-01-01' and '1998-12-31'
              |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
              |group by
              |  i_item_id
              |order by
              |  i_item_id
              |limit 100
              |-- end query 1 in stream 0 using template query7.tpl
            """.stripMargin),
            
      ("store_sales-selfjoin-1",   """
                                   |-- The join condition will yield many matches.
                                   |select
                                   |  t1.ss_quantity,
                                   |  t1.ss_list_price,
                                   |  t1.ss_coupon_amt,
                                   |  t1.ss_cdemo_sk,
                                   |  t1.ss_item_sk,
                                   |  t1.ss_promo_sk,
                                   |  t1.ss_sold_date_sk
                                   |from store_sales t1 join store_sales t2 on t1.ss_item_sk = t2.ss_item_sk
                                   |where
                                   |  t1.ss_sold_date_sk between 2450815 and 2451179
                                   """.stripMargin),

	   
      ("store_sales-selfjoin-2",   """
                                   |-- We ust comound primary key as the join condition. The size of output is comparable with the input table.
                                   |select
                                   |  t1.ss_quantity,
                                   |  t1.ss_list_price,
                                   |  t1.ss_coupon_amt,
                                   |  t1.ss_cdemo_sk,
                                   |  t1.ss_item_sk,
                                   |  t1.ss_promo_sk,
                                   |  t1.ss_sold_date_sk
                                   |from store_sales t1 join store_sales t2 on t1.ss_item_sk = t2.ss_item_sk and t1.ss_ticket_number = t2.ss_ticket_number
                                   |where
                                   |  t1.ss_sold_date_sk between 2450815 and 2451179
                                   """.stripMargin)
   ).map { case (name, sqlText) =>
     Query(name = name, sqlText = sqlText, description = "", executionMode = ForeachResults)
   }
}
