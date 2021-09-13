package com.tairong.writer

import com.tairong.common.{FileBaseSinkConfigEntry, TagConfigEntry, TrContext}
import com.tairong.utils.{FileUtils, HDFSUtils}
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class CsvWriter(data: DataFrame, config: TagConfigEntry, tbName: String) extends Writer {
  private[this] val LOG = Logger.getLogger(this.getClass)

  override def write(): Unit = {
    val sinkConfig = config.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
    val hdfsSrcPath = sinkConfig.srcPath
    //    val localFilePath = sinkConfig.targetPath
    val nameNode = sinkConfig.fsName.orNull
    val count = data.count()
    LOG.info(s"data.count: " + count)

    //    FileUtils.delete(localFilePath)
    val csvPath = hdfsSrcPath + "/" + tbName + ".csv"

    val tmpCsvFolder = hdfsSrcPath + "_tmp"

    data.coalesce(1)
      .write.option("header", "true")
      .option("delimiter", "^")
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .csv(s"$tmpCsvFolder")

    val fs = HDFSUtils.getFileSystem(nameNode)
    val files = fs.globStatus(new Path(tmpCsvFolder + "/part*.csv"))
    files.foreach(file => {
      val fileName = file.getPath.getName
      if (!HDFSUtils.exists(hdfsSrcPath)) {
        HDFSUtils.create(hdfsSrcPath, nameNode)
      }
      LOG.info(s"tmpCsvFolder-file: " + tmpCsvFolder + "/" + file)
      fs.rename(new Path(tmpCsvFolder + "/" + fileName), new Path(csvPath))
      LOG.info(s"rename-after: " + csvPath)
    })
    fs.delete(new Path(tmpCsvFolder), true)
  }
}
