package com.tairong.reader

import com.tairong.common.FileBaseSourceConfigEntry
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The FileBaseReader is the abstract class for HDFS file reader.
 *
 * @param session
 * @param path
 */
abstract class FileBaseReader(val session: SparkSession, val path: String) extends Reader {

  require(path.trim.nonEmpty)

  override def close(): Unit = {
    session.close()
  }
}

/**
 * The ParquetReader extend the FileBaseReader and support read parquet file from HDFS.
 *
 * @param session
 * @param parquetConfig
 */
class ParquetReader(override val session: SparkSession, parquetConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, parquetConfig.path) {

  override def read(): DataFrame = {
    session.read.parquet(path)
  }
}

/**
 * The ORCReader extend the FileBaseReader and support read orc file from HDFS.
 *
 * @param session
 * @param orcConfig
 */
class ORCReader(override val session: SparkSession, orcConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, orcConfig.path) {

  override def read(): DataFrame = {
    session.read.orc(path)
  }
}

/**
 * The JSONReader extend the FileBaseReader and support read json file from HDFS.
 *
 * @param session
 * @param jsonConfig
 */
class JSONReader(override val session: SparkSession, jsonConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, jsonConfig.path) {

  override def read(): DataFrame = {
    session.read.json(path)
  }
}

/**
 * The CSVReader extend the FileBaseReader and support read csv file from HDFS.
 * All types of the structure are StringType.
 *
 * @param session
 * @param csvConfig
 */
class CSVReader(override val session: SparkSession, csvConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, csvConfig.path) {

  override def read(): DataFrame = {
    session.read
      .option("delimiter", csvConfig.separator.get)
      .option("header", csvConfig.header.get)
      .option("emptyValue", "DEFAULT_EMPTY_VALUE")
      .csv(path)
  }

}
