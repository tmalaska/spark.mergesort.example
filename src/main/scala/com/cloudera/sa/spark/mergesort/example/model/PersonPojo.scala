package com.cloudera.sa.spark.mergesort.example.model

import org.apache.spark.sql.Row

import scala.collection.mutable

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

    val trans = row.get(1).asInstanceOf[mutable.WrappedArray[Row]].map(r => {
      TranPojoBuilder.build(r)
    })

    new PersonPojo(row.getLong(0), trans.toArray)
  }
}
