package com.google.cloud.spark.spanner.tpch

import com.google.cloud.spark.spanner.tpch.model.{Customer, Lineitem}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object TPCHApp extends App {


    println("Start")

    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    println(s"Running Spark ${spark.version}")

    val inDir = "/Users/thrynchuk/Documents/repo-spark-conn/cloud-spanner-spark-connector/src/test/resources/tpch"
    val parquet = false

    createViews(spark, inDir, parquet)
    val queries = Seq(
      (query1 _, 1),
      //    (query2 _, 2),
      //    (query3 _, 3),
      //    (query4 _, 4),
      //    (query5 _, 5),
      //    (query6 _, 6),
      //    (query7 _, 7),
      //    (query8 _, 8),
      //    (query9 _, 9),
      //    (query10 _, 10),
      //    (query11 _, 11),
      //    (query12 _, 12),
      //    (query13 _, 13),
      //    (query14 _, 14),
      //    (query15 _, 15),
      //    (query16 _, 16),
      //    (query17 _, 17),
      //    (query18 _, 18),
      //    (query19 _, 19),
      //    (query20 _, 20),
      //    (query21 _, 21),
      //    (query22 _, 22)
    )

    val tableMap: Map[Int, Seq[String]] = Map(
      1 -> Seq("lineitem"),
      //    2 -> Seq("part", "supplier", "partsupp", "nation", "region"),
      //    3 -> Seq("customer", "orders", "lineitem"),
      //    4 -> Seq("orders", "lineitem"),
      //    5 -> Seq("customer", "orders", "lineitem", "supplier", "nation", "region"),
      //    6 -> Seq("lineitem"),
      //    7 -> Seq("supplier", "lineitem", "orders", "customer", "nation"),
      //    8 -> Seq("part", "supplier", "lineitem", "orders", "customer", "nation", "region"),
      //    9 -> Seq("part", "supplier", "lineitem", "partsupp", "orders", "nation"),
      //    10 -> Seq("customer", "orders", "lineitem", "nation"),
      //    11 -> Seq("partsupp", "supplier", "nation"),
      //    12 -> Seq("orders", "lineitem"),
      //    13 -> Seq("customer", "orders"),
      //    14 -> Seq("lineitem", "part"),
      //    15 -> Seq("lineitem", "supplier"),
      //    16 -> Seq("partsupp", "part", "supplier"),
      //    17 -> Seq("lineitem", "part"),
      //    18 -> Seq("customer", "orders", "lineitem"),
      //    19 -> Seq("lineitem", "part"),
      //    20 -> Seq("supplier", "nation", "partsupp", "part", "lineitem"),
      //    21 -> Seq("supplier", "lineitem", "orders", "nation"),
      //    22 -> Seq("customer", "orders")
    )


    queries.foreach { case (query, i) =>
      cacheTables(spark, tableMap, i)
     // benchmark(spark, i, query, true)
    }


  var first = true
  def benchmark(sparkSession: SparkSession, i: Int, f: SparkSession => (Array[_], DataFrame), skipPlan: Boolean): Unit = {
    if (first) {
      println("Warming up JVM...")
      val df = sparkSession.sql("SELECT max(l_partkey), min(l_orderkey), avg(l_tax) from lineitem")
      df.collect().foreach(println)
      System.gc()
      first = false
    }

    println(s"Running Query${i}")
//    logger.info(s"Running Query${i}")

    val start = System.nanoTime()
    val (res, ds) = f(sparkSession)
    val end = System.nanoTime()

    val duration = (end - start).toDouble / 1e9

    println(s"Result returned ${res.length} records.")
//    logger.info(s"Result returned ${res.length} records.")

    println(s"Query${i} elapsed: ${duration} s")
//    logger.info(s"Query time: ${duration}")

    res.take(10).foreach(println)

    val usedVe = ds.queryExecution.executedPlan.toString().contains("SparkToVectorEngine")
    println(s"Used VE: $usedVe")

//    if (!skipPlan) {
//      logPlan(ds.queryExecution.executedPlan)
//    }
  }

  def cacheTables(sparkSession: SparkSession, tableMap: Map[Int, Seq[String]], i : Int): Unit = {
    for (key <- tableMap(i)) {
      println(s"Caching Table ${key}")

      sparkSession.sql("CACHE TABLE " + key)
      //sparkSession.sql("ANALYZE TABLE " + key + " COMPUTE STATISTICS FOR ALL COLUMNS")
    }
  }

  def query1(sparkSession: SparkSession): (Array[_], DataFrame) = {
    val delta = 90
    val sql = s"""
      select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
        sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
      from
        lineitem
      where
        l_shipdate <= date '1998-12-01' - interval '$delta' day
      group by l_returnflag, l_linestatus
      order by l_returnflag, l_linestatus
      limit 1
    """

    val ds = sparkSession.sql(sql)
    val res = ds.limit(1).collect()

    (res, ds)
  }

  def createViews(sparkSession: SparkSession, inputDir: String, parquet: Boolean): Unit = {
    import sparkSession.implicits._

    val dfMap = Map[String, SparkContext => DataFrame](
//      "customer" -> (sc =>
//        sc.textFile(inputDir + "/customer.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p =>
//            Customer(
//              p(0).trim.toLong,
//              p(1).trim,
//              p(2).trim,
//              p(3).trim.toLong,
//              p(4).trim,
//              p(5).trim.toDouble,
//              p(6).trim,
//              p(7).trim
//            )
//          )
//          .toDF()),
      "lineitem" -> (sc =>
        sc.textFile(inputDir + "/lineitem.tbl*", minPartitions = 8)
          .map(_.split('|'))
          .map(p =>
            Lineitem(
              l_orderkey = p(0).trim.toLong,
              l_partkey = p(1).trim.toLong,
              l_suppkey = p(2).trim.toLong,
              l_linenumber = p(3).trim.toLong,
              l_quantity = p(4).trim.toDouble,
              l_extendedprice = p(5).trim.toDouble,
              l_discount = p(6).trim.toDouble,
              l_tax = p(7).trim.toDouble,
              l_returnflag = p(8).trim.toCharArray.apply(0),
              l_linestatus = p(9).trim.toCharArray.apply(0),
              l_shipdate =  java.sql.Date.valueOf(p(10).trim),
              l_commitdate = java.sql.Date.valueOf(p(11).trim),
              l_receiptdate = java.sql.Date.valueOf(p(12).trim),
              l_shipinstruct = p(13).trim,
              l_shipmode = p(14).trim,
              l_comment = p(15).trim
            )
          )
          .toDF()),
//      "nation" -> (sc =>
//        sc.textFile(inputDir + "/nation.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim))
//          .toDF()),
//      "region" -> (sc =>
//        sc.textFile(inputDir + "/region.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p => Region(p(0).trim.toLong, p(1).trim, p(2).trim))
//          .toDF()),
//      "orders" -> (sc =>
//        sc.textFile(inputDir + "/orders.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p =>
//            Order(
//              p(0).trim.toLong,
//              p(1).trim.toLong,
//              p(2).trim,
//              p(3).trim.toDouble,
//              p(4).trim,
//              p(5).trim,
//              p(6).trim,
//              p(7).trim.toLong,
//              p(8).trim
//            )
//          )
//          .toDF()),
//      "part" -> (sc =>
//        sc.textFile(inputDir + "/part.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p =>
//            Part(
//              p(0).trim.toLong,
//              p(1).trim,
//              p(2).trim,
//              p(3).trim,
//              p(4).trim,
//              p(5).trim.toLong,
//              p(6).trim,
//              p(7).trim.toDouble,
//              p(8).trim
//            )
//          )
//          .toDF()),
//      "partsupp" -> (sc =>
//        sc.textFile(inputDir + "/partsupp.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p =>
//            Partsupp(
//              p(0).trim.toLong,
//              p(1).trim.toLong,
//              p(2).trim.toLong,
//              p(3).trim.toDouble,
//              p(4).trim
//            )
//          )
//          .toDF()),
//      "supplier" -> (sc =>
//        sc.textFile(inputDir + "/supplier.tbl*", minPartitions = 8)
//          .map(_.split('|'))
//          .map(p =>
//            Supplier(
//              p(0).trim.toLong,
//              p(1).trim,
//              p(2).trim,
//              p(3).trim.toLong,
//              p(4).trim,
//              p(5).trim.toDouble,
//              p(6).trim
//            )
//          )
//          .toDF())
    )

    dfMap.foreach { case (key, value) =>
      val sc = sparkSession.sparkContext
      if (parquet) {
        println("Using Parquet")
        val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
        if (!fs.exists(new org.apache.hadoop.fs.Path(s"${inputDir}/${key}.parquet"))) {
          println(s"Writing ${inputDir}/${key}.parquet")

          value(sc).write.parquet(s"${inputDir}/${key}.parquet")
        }

        val parquetTable = sparkSession.read.parquet(s"${inputDir}/${key}.parquet")
        parquetTable.createOrReplaceTempView(key)
      } else {
        println(s"reading $key CSV")
        value(sc).createOrReplaceTempView(key)

      }
    }
  }

}
