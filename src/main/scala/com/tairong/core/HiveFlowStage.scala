package com.tairong.core

import com.tairong.common.{TagConfigEntry, TrContext}
import com.tairong.processor.HiveProcessor
import com.tairong.writer.{CsvWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class HiveFlowStage(df: DataFrame, config: TagConfigEntry, tbName: String) extends FlowStage {
  private[this] val LOG = Logger.getLogger(this.getClass)

  override def run_(context: TrContext): Unit = {
    LOG.info(s"begin to execute HiveFlowStage")
    processor = new HiveProcessor(df)
    val processedData = processor.process()
    //这里不直接创建HiveOutputWriter,通过
    //    new HiveWriterFactory().getWriter()
    val writer = new CsvWriter(processedData, config, tbName)
    writer.write()
  }

}
