package com.cloudera.sa.spark.mergesort.example

import java.util.{Date, Random}

import com.cloudera.sa.spark.mergesort.example.model.{AccountPojo, PersonPojo, TranPojo}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DataGenerator {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<runLocal> " +
        "<numOfAccounts> " +
        "<numOfPersons> " +
        "<numOfTrans> " +
        "<outputTable> " +
        "<outputFolder>")
      return
    }

    val runLocal = args(0).equals("l")
    val numOfAccounts = args(1).toInt
    val numOfPersons = args(2).toInt
    val numOfTrans = args(3).toInt
    val outputTable = args(4)
    val outputFolder = args(5)

    val sc:SparkContext = if (runLocal) {
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

    val arrayOfAccounts = new Array[AccountPojo](numOfAccounts)
    val r = new Random()
    var time = (new Date).getTime

    for (a <- 0 until numOfAccounts) {
      val arrayOfPersons = new Array[PersonPojo](numOfPersons)
      for (p <- 0 until numOfPersons) {
        val arrayOfTrans = new Array[TranPojo](numOfTrans)
        for (t <- 0 until numOfTrans) {
          time += 10000

          arrayOfTrans(t) = new TranPojo(t, r.nextInt(), time)
        }
        arrayOfPersons(p) = new PersonPojo(p, arrayOfTrans)
      }
      arrayOfAccounts(a) = new AccountPojo(a, arrayOfPersons)
    }

    val rowRdd = sc.parallelize(arrayOfAccounts)map(r => {
      r.toRow
    })

    sqlContext.sql("create external table " + outputTable + " ( " +
      " account_id BIGINT, " +
      " person ARRAY<STRUCT< " +
      "   person_id: BIGINT, " +
      "   tran: ARRAY<STRUCT< " +
      "     tran_id: BIGINT, " +
      "     amount: DOUBLE, " +
      "     datetime: BIGINT " +
      "   >> " +
      " >>) " +
      " STORED AS PARQUET " +
      " LOCATION '" + outputFolder + "'")

    val emptyRdd = sqlContext.sql("select * from " + outputTable + " limit 0")

    sqlContext.createDataFrame(rowRdd, emptyRdd.schema).registerTempTable("tempTable")

    sqlContext.sql("insert into table " + outputTable + " select * from tempTable")

    sqlContext.sql("select * from " + outputTable + " limit 5").collect().foreach(println)

    sc.stop()

  }
}
