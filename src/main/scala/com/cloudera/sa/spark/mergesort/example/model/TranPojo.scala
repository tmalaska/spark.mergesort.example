package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

class TranPojo(val tranId:Long,
               val amount:Double,
               val datetime:Long) extends Serializable {
  def toRow: Row = {
    Row(tranId, amount, datetime)
  }
}

object TranPojoBuilder {
  def build(row:Row): TranPojo = {
    new TranPojo(row.getLong(0),
      row.getDouble(1),
      row.getLong(2))
  }
}
