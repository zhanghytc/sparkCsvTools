package com.tairong.reader

import org.apache.spark.sql.{DataFrame}

trait Reader extends Serializable {
  def read(): DataFrame

  def close(): Unit

}
