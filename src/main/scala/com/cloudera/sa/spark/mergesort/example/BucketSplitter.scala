package com.cloudera.sa.spark.mergesort.example

import java.util

import com.cloudera.sa.spark.mergesort.example.partitioner.InitialBucketingPartitioner
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BucketSplitter {
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

    val inputRdd = sqlContext.sql("select * from " + inputTable)

    val firstRecords = new util.HashMap[Long, Int]()

    inputRdd.mapPartitions(it => {
      val firstRecord = it.next()
      val firstAccountId = firstRecord.getLong(firstRecord.fieldIndex("account_id"))
      val count = it.count(r => true)
      Seq((firstAccountId, count)).iterator
    }).collect().foreach(r => {
      firstRecords.put(r._1, r._2)
    })

    val firstRecordsBc = sc.broadcast(firstRecords)

    val firstHalf = inputRdd.mapPartitions(it => {
      var isFirst = true
      var counter = -1
      var midCount = 0
      it.map(r => {
        if (isFirst) {
          isFirst = false
          midCount = firstRecordsBc.value.get(r.getLong(r.fieldIndex("account_id"))) / 2
        }
        counter += 1
        if (counter < midCount) {
          r
        } else {
          null
        }
      })
    }).filter(r => r != null)

    val secondHalf = inputRdd.mapPartitions(it => {
      var isFirst = true
      var counter = -1
      var midCount = 0
      it.map(r => {
        if (isFirst) {
          isFirst = false
          midCount = firstRecordsBc.value.get(r.getLong(r.fieldIndex("account_id"))) / 2
        }
        counter += 1
        if (counter >= midCount) {
          r
        } else {
          null
        }
      })
    }).filter(r => r != null)

    val newWhole = firstHalf.union(secondHalf)

    val emptyRdd = sqlContext.sql("select * from " + outputTable + " limit 0")

    sqlContext.createDataFrame(newWhole, emptyRdd.schema).registerTempTable("tempTable")

    sqlContext.sql("insert into table " + outputTable + " select * from tempTable")

    sqlContext.sql("select * from " + outputTable + " limit 5").collect().foreach(println)

  }
}


