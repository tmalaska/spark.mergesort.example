package com.cloudera.sa.spark.mergesort.example

import com.cloudera.sa.spark.mergesort.example.partitioner.MergeBucketingPartitioner
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BucketedSortedMerge {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<runLocal> " +
        "<numOfSalts> " +
        "<inputExistingTable> " +
        "<inputNewTable> " +
        "<typeFolder> " +
        "<outputTable> " +
        "<outputFolder> ")
      return
    }

    val runLocal = args(0).equals("l")
    val numOfSalts = args(1).toInt
    val inputExistingTable = args(2)
    val inputNewTable = args(3)
    val typeFolder = args(4)
    val outputTable = args(5)
    val outputFolder = args(6)

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

    val inputExistingRdd = sqlContext.sql("select * from " + inputExistingTable)
    var partitionCounter = -1

    val firstRecords:Array[(Long, Int)] = inputExistingRdd.mapPartitions(it => {
      val firstRecord = it.next()

      val firstAccountId = firstRecord.getLong(firstRecord.fieldIndex("account_id"))

      Seq(firstAccountId).iterator
    }).collect.map(r => {
      partitionCounter += 1
      (r, partitionCounter)
    })

    val firstRecordsBc = sc.broadcast(firstRecords)

    val inputNewDf = sqlContext.sql("select * from " + inputNewTable)

    val bucktedAndSortedInputNewRdd = inputNewDf.map(r => {
      (r.getLong(r.fieldIndex("account_id")), r)
    }).repartitionAndSortWithinPartitions(
      new MergeBucketingPartitioner(numOfSalts, firstRecordsBc)).map(r => {
      r._2
    })

    sqlContext.createDataFrame(bucktedAndSortedInputNewRdd, inputExistingRdd.schema).
      write.parquet(typeFolder)



    sc.stop

  }
}
