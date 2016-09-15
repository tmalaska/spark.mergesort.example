package com.cloudera.sa.spark.mergesort.example

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object ViewData {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<runLocal> " +
        "<table> " +
        "<limit> ")
      return
    }

    val runLocal = args(0).equals("l")
    val inputTable = args(1)
    val limit = args(2)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "DataGenerator", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("DataGenerator")
      new SparkContext(sparkConf)
    }

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("select * from " + inputTable + " limit " + limit).collect().foreach(println)

    sc.stop()
  }
}
