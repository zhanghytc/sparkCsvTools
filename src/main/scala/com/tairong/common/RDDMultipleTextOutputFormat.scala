package com.tairong.common

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[String, String] {
  //  文件名 ： 指定为 k.csv
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = key+".csv"
}
