package com.tairong.processor

import org.apache.spark.sql.DataFrame

trait Processor extends Serializable {

  def process(): DataFrame

}
