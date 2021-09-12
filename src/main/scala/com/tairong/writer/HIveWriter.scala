package com.tairong.writer

import com.tairong.common.{HiveSinkConfigEntry, TagConfigEntry, TrContext}
import org.apache.spark.sql.DataFrame

class HIveWriter (data:DataFrame, trContext: TrContext, config: TagConfigEntry) extends Writer {

  override def write(): Unit = {
    val hiveSinkConfig = config.dataSinkConfigEntry.asInstanceOf[HiveSinkConfigEntry]
    val tableName = hiveSinkConfig.tableName
    data.write.mode("append")
      .saveAsTable(tableName)

    trContext.spark.close()
  }
}
