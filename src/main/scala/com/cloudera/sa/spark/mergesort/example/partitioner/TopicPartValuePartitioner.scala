package com.cloudera.sa.spark.mergesort.example.partitioner

import org.apache.spark.Partitioner

class TopicPartValuePartitioner(val numOfKafkaParts:Int, val numOfThreadsPerKafkaPart:Int) extends Partitioner {
  override def numPartitions: Int = numOfKafkaParts * numOfThreadsPerKafkaPart

  override def getPartition(key: Any): Int = {
    //Topic, PartitionNum, Value
    val keyTuple = key.asInstanceOf[(String, Int, String)]

    Math.abs((keyTuple._1.hashCode + keyTuple._2.hashCode() + keyTuple._3.hashCode % numOfThreadsPerKafkaPart) % numPartitions)
  }
}