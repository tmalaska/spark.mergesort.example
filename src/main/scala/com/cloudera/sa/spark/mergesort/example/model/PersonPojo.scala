package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

class PersonPojo(val personId:Long,
                 val trans:Array[TranPojo]) extends Serializable {
  def toRow: Row = {

    val transRowSeq = trans.map(r => {
      r.toRow
    })

    Row(personId, transRowSeq)
  }

  def + (other:PersonPojo): PersonPojo = {
    new PersonPojo(personId, trans ++ other.trans)
  }
}

object PersonPojoBuilder {
  def build(row:Row): PersonPojo = {

    val trans = row.getSeq(1).map(r => {
      TranPojoBuilder.build(r)
    })

    new PersonPojo(row.getLong(0), trans.toArray)
  }
}
