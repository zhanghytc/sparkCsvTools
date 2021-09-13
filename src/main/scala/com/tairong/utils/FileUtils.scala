package com.tairong.utils

import java.io.File

import org.apache.log4j.Logger

object FileUtils {
  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  def delete(path: String): Unit = {
    val file = new File(path)
    if (file.delete)
      LOG.info(file.getName + "is deleted")
    else
      LOG.info(file.getName + "Delete failed")
  }
}
