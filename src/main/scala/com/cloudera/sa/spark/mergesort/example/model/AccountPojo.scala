package com.cloudera.sa.spark.mergesort.example.model

import java.util

import org.apache.spark.sql.Row

import scala.collection.mutable

class AccountPojo(val accountId:Long,
                  val persons:Array[PersonPojo]) extends Serializable {
  def toRow: Row = {

    val personRowSeq = persons.map(r => {
      r.toRow
    })

    Row(accountId, personRowSeq)
  }

  def + (other:AccountPojo): AccountPojo = {
    val lookupMap = new util.HashMap[Long, PersonPojo]
    persons.foreach(r => {
      lookupMap.put(r.personId, r)
    })

    val newPersonList = new mutable.MutableList[PersonPojo]

    other.persons.foreach(r => {
      val existingPerson = lookupMap.get(r.personId)
      if (existingPerson != null) {
        newPersonList += (r + existingPerson)
        lookupMap.remove(r.personId)
      } else {
        newPersonList += r
      }
    })

    val it = lookupMap.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      newPersonList += e.getValue
    }

    new AccountPojo(accountId, newPersonList.toArray)
  }
}

object AccountPojoBuilder {
  def build(row:Row): AccountPojo = {


    val persons = row.get(1).asInstanceOf[mutable.WrappedArray[Row]].map(r => {
      PersonPojoBuilder.build(r)
    })

    new AccountPojo(row.getLong(0), persons.toArray)

  }
}
