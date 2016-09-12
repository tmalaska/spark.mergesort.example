package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

class TranPojo(val tranId:Long,
               val amount:Double,
               val datetime:Long) {
  def toRow: Row = {
    Row(tranId, amount, datetime)
  }
}
