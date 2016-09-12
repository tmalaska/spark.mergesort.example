package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

class PersonPojo(val personId:Long,
                 val trans:Array[TranPojo]) {
  def toRow: Row = {

    val transRowSeq = trans.map(r => {
      r.toRow
    })

    Row(personId, transRowSeq)
  }
}
