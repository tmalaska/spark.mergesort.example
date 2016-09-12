package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

class AccountPojo(val accountId:Long,
                  val person:Array[PersonPojo]) {
  def toRow: Row = {

    val personRowSeq = person.map(r => {
      r.toRow
    })

    Row(accountId, personRowSeq)
  }
}
