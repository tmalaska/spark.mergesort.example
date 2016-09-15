package com.cloudera.sa.spark.mergesort.example

import com.cloudera.sa.spark.mergesort.example.model.{AccountPojo, AccountPojoBuilder}
import com.cloudera.sa.spark.mergesort.example.partitioner.MergeBucketingPartitioner
import org.apache.commons.lang.{SerializationUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext


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

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("create external table if not exists " + outputTable + " ( " +
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

    val bucktedAndSortedInputNewRdd = inputNewDf.
      map(r => {
      (r.getLong(r.fieldIndex("account_id")), r)
    }).
      repartitionAndSortWithinPartitions(new MergeBucketingPartitioner(numOfSalts, firstRecordsBc)).
      map(r => {
      ("", SerializationUtils.serialize(AccountPojoBuilder.build(r._2)))
    })

    val sample = bucktedAndSortedInputNewRdd.take(10)
    sample.foreach(r => {
      println
    })

    bucktedAndSortedInputNewRdd.saveAsSequenceFile(typeFolder)

    val inputExistingDf = sqlContext.sql("select * from " + inputExistingTable)

    val mergedOutputRdd:RDD[org.apache.spark.sql.Row] = inputExistingDf.mapPartitions(it => {

      new MergeIterator(it,
        numOfSalts,
        firstRecordsBc,
        typeFolder)
    })

    sqlContext.createDataFrame(mergedOutputRdd, inputExistingDf.schema).
      registerTempTable("mergedTemp")

    sqlContext.sql("insert into " + outputTable + " select * from mergedTemp")

    sqlContext.sql("select * from " + outputTable + " limit 5").collect().foreach(println)

    sc.stop
  }
}

class MergeIterator (it:Iterator[org.apache.spark.sql.Row],
                         numOfSalts:Int,
                         firstRecordsBc:Broadcast[Array[(Long,Int)]],
                         typeFolder:String) extends Iterator[Row] {

  var isFirst = true
  var currentSeqAccount:AccountPojo = null
  var existingAccount:AccountPojo = null
  var lastWasFromExisting = true
  val fs = StaticFileSystem.getFileSystem()
  val valueWritable = new BytesWritable()
  val keyWriteable = new Text()
  var reader:SequenceFile.Reader = null
  val nextValue:AccountPojo = null

  override def hasNext: Boolean = {
    if (existingAccount == null) {
      if (it.hasNext) {
        existingAccount = AccountPojoBuilder.build(it.next())

        if (isFirst) {
          isFirst = false
          val partitioner = new MergeBucketingPartitioner(numOfSalts, firstRecordsBc)
          val thisPartition = partitioner.getPartition(existingAccount.accountId)

          val seqPath = new Path(typeFolder + "/part-" + StringUtils.leftPad(thisPartition.toString, 5, '0'))
          reader = new SequenceFile.Reader(fs, seqPath, fs.getConf)

          if (reader.next(keyWriteable, valueWritable)) {
            currentSeqAccount = SerializationUtils.deserialize(valueWritable.copyBytes()).asInstanceOf[AccountPojo]
          }
        }
      }
    }

    if (currentSeqAccount == null) {
      if (reader.next(keyWriteable, valueWritable)) {
        currentSeqAccount = SerializationUtils.deserialize(valueWritable.copyBytes()).asInstanceOf[AccountPojo]
      }
    }
    currentSeqAccount != null || existingAccount != null
  }

  override def next(): org.apache.spark.sql.Row = {
    var result:org.apache.spark.sql.Row = null
    if ((existingAccount == null && currentSeqAccount != null) ||
      (currentSeqAccount != null && currentSeqAccount.accountId < existingAccount.accountId)) {
      result = currentSeqAccount.toRow
      currentSeqAccount = null
    } else if (existingAccount != null && currentSeqAccount != null && currentSeqAccount.accountId == existingAccount.accountId) {
      result = (currentSeqAccount + existingAccount).toRow
      currentSeqAccount = null
      existingAccount = null
    } else if (existingAccount != null) {
      result = existingAccount.toRow
      existingAccount = null
    }
    result
  }
}
