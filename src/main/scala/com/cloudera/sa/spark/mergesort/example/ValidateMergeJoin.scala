package com.cloudera.sa.spark.mergesort.example

import com.cloudera.sa.spark.mergesort.example.model.AccountPojoBuilder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ValidateMergeJoin {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<runLocal> " +
        "<inputExitingTable> " +
        "<inputNewTable> " +
        "<inputMergeTable> ")
      return
    }

    val runLocal = args(0).equals("l")
    val inputExistingTable = args(1)
    val inputNewTable = args(2)
    val inputMergeTable = args(3)

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

    val existingDf = sqlContext.sql("select * from " + inputExistingTable)
    val newDf = sqlContext.sql("select * from " + inputNewTable)
    val mergedDf = sqlContext.sql("select * from " + inputMergeTable)

    val mergedOldKeyValueRdd = existingDf.unionAll(newDf).map(r => {
      val accountId = r.getLong(r.fieldIndex("account_id"))
      (accountId, AccountPojoBuilder.build(r))
    }).reduceByKey((a, b) => a + b)

    val mergedNewKeyValueRdd = mergedDf.map(r => {
      val accountId = r.getLong(r.fieldIndex("account_id"))
      (accountId, AccountPojoBuilder.build(r))
    })

    val nonMatches = mergedOldKeyValueRdd.leftOuterJoin(mergedNewKeyValueRdd).map(r => {
      if (r._2._2.isEmpty) {
        (r._2._1.hashCode(), (r._2._1, null))
      } else {
        (r._2._1.hashCode() - r._2._2.get.hashCode(), (r._2._1, r._2._2.get))
      }

    }).filter(r => {
      r._1 != 0
    })

    val countOfNonMatches = nonMatches.count()
    val countOfMergedRecords = mergedOldKeyValueRdd.count()

    if (countOfNonMatches > 0) {
      nonMatches.take(10).foreach(println)
      println("----")
      println("countOfNonMatches:" + countOfNonMatches)
      println("----")
    } else {
      println("----")
      println("All is good")
      println("countOfMergedRecords:" + countOfMergedRecords)
      println("----")
    }

  }
}
