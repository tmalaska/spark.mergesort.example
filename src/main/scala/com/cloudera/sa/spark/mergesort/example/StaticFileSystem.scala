package com.cloudera.sa.spark.mergesort.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object StaticFileSystem {
  var fs:FileSystem = null

  def getFileSystem(): FileSystem = {
    this.synchronized {
      if (fs == null) {
        fs = FileSystem.get(new Configuration)
      }
    }
    fs
  }
}
