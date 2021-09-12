package com.tairong.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame}


class HiveProcessor(data: DataFrame) extends Processor {

  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  override def process(): DataFrame = {
    LOG.info("begin to execute HiveProcessor")
    data.show(100)
    data
  }
}
