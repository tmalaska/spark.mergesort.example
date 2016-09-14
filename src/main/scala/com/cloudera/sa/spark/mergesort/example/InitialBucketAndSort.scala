package com.cloudera.sa.spark.mergesort.example

import com.cloudera.sa.spark.mergesort.example.partitioner.InitialBucketingPartitioner
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object InitialBucketAndSort {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<runLocal> " +
        "<numOfSalts> " +
        "<inputTable> " +
        "<outputTable> " +
        "<outputFolder> ")
      return
    }

    val runLocal = args(0).equals("l")
    val numOfSalts = args(1).toInt
    val inputTable = args(2)
    val outputTable = args(3)
    val outputFolder = args(4)

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

    val sqlContext = new SQLContext(sc)

    sqlContext.sql("create external table " + outputTable + " ( " +
      " account_id BIGINT, " +
      " person ARRAY<STRUCT< " +
      "   person_id: BIGINT, " +
      "   tran: ARRAY<STRUCT< " +
      "     tran_id: BIGINT, " +
      "     amount: DOUBLE, " +
      "     datetime: BIGINT " +
      "   >> " +
      " >> " +
      " STORED AS PARQUET " +
      " LOCATION '" + outputFolder + "'")

    val bucketedSortedRowRdd = sqlContext.sql("select * from " + inputTable).map(r => {
      val accountId = r.getLong(r.fieldIndex("account_id"))

      val saltedKey = Math.abs(accountId.hashCode() % numOfSalts)

      ((saltedKey, accountId), r)
    }).repartitionAndSortWithinPartitions(new InitialBucketingPartitioner(numOfSalts)).map(r => {
      r._2
    })

    val emptyRdd = sqlContext.sql("select * from " + outputTable + " limit 0")

    sqlContext.createDataFrame(bucketedSortedRowRdd, emptyRdd.schema).registerTempTable("tempTable")

    sqlContext.sql("insert into table " + outputTable + " select * from tempTable")

    sqlContext.sql("select * from " + outputTable + " limit 5").collect().foreach(println)

  }
}
