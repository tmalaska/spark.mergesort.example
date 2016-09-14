package com.cloudera.sa.spark.mergesort.example.partitioner

import org.apache.spark.Partitioner

class InitialBucketingPartitioner(val numOfSalts:Int) extends Partitioner {
  override def numPartitions: Int = numOfSalts

  override def getPartition(key: Any): Int = {
    val saltedKey = key.asInstanceOf[(Int, Long)]
    saltedKey._1
  }
}
