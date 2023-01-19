package com.google.cloud.spark.spanner.tpch.model

import java.time.LocalDate

case class Lineitem(
                     l_orderkey: Long,
                     l_partkey: Long,
                     l_suppkey: Long,
                     l_linenumber: Long,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: Long,
                     l_linestatus: Long,
                     l_shipdate: java.sql.Date, //LocalDate,
                     l_commitdate: java.sql.Date, //LocalDate,
                     l_receiptdate: java.sql.Date, //LocalDate,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String
                   )
