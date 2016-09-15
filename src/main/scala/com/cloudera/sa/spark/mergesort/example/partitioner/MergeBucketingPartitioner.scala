package com.cloudera.sa.spark.mergesort.example.partitioner

import java.util

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast

class MergeBucketingPartitioner(val numOfSalts:Int, val firstRecordsBc:Broadcast[Array[(Long, Int)]]) extends Partitioner {
  override def numPartitions: Int = numOfSalts

  @transient val saltedMap = new util.HashMap[Int, Array[(Long, Int)]]

  firstRecordsBc.value.foreach(r => {
    val saltedKey = Math.abs(r._1 % numOfSalts).toInt

    val keys:Array[(Long, Int)] = saltedMap.getOrDefault(saltedKey, new Array[(Long, Int)](0))
    saltedMap.put(saltedKey, keys :+ r)
  })


  val sortedSaltedMap = new util.HashMap[Int, Array[(Long, Int)]]
  @transient val it = saltedMap.entrySet().iterator()

  while (it.hasNext) {
    val entry = it.next()

    val sortedList = entry.getValue

    sortedSaltedMap.put(entry.getKey, sortedList.sortBy(r => r))
  }

  @transient val it2 = sortedSaltedMap.entrySet().iterator()
  while (it.hasNext) {
    val entry = it.next()
    println(" - " + entry.getKey + ":")
    entry.getValue.foreach(r => {
      println(" -- " + r._1 + "," + r._2)
    })
  }

  override def getPartition(key: Any): Int = {
    val longKey = key.asInstanceOf[Long]
    val saltedKey = Math.abs(longKey.hashCode() % numOfSalts)

    val sortedFirstRows = sortedSaltedMap.get(saltedKey)

    if (sortedFirstRows == null) {
      println("No salted key found")
      println("longKey:" + longKey)
      println("numOfSalts:" + numOfSalts)
      println("saltedKey:" + saltedKey)
      println("sortedFirstRows:" + sortedFirstRows)
      val it = sortedSaltedMap.entrySet().iterator()
      while (it.hasNext) {
        val entry = it.next()
        println(" - " + entry.getKey + ":")
        entry.getValue.foreach(r => {
          println(" -- " + r._1 + "," + r._2)
        })
      }
    }

    var counter = 0
    var partition = -1
    while (counter < sortedFirstRows.length && longKey >= sortedFirstRows(counter)._1) {
      partition = sortedFirstRows(counter)._2
      counter += 1
    }

    partition
  }
}