package com.tairong

import java.io.File

import com.tairong.common._
import com.tairong.core.HiveFlowStage
import com.tairong.reader.{HiveReader, ParquetReader}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


/**
 * TrSparkCsvTools is a simple spark job used to read hive table and write data into csv file.
 */
object TrSparkCsvTools {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "TrSparkCsvTools"
    val options = TrConfig.parser(args, PROGRAM_NAME)
    val c: Argument = options match {
      case Some(config) => config
      case _ =>
        LOG.error("Argument parse failed")
        sys.exit(-1)
    }
    val configs: Configs = TrConfig.parse(new File(c.config))

    //    val spark = TrSparkConf.createSparkContext(appName,configs)
    val spark = TrSparkConf.createSparkSession(c, configs, PROGRAM_NAME)
    println(s"configs: $configs")
    println(s"configs.tagsConfig: ${configs.tagsConfig}")
    LOG.info(s"configs: $configs")
    LOG.info(s"configs.tagsConfig: ${configs.tagsConfig}")
    val trContext = TrContext(spark, configs)
    if (configs.tagsConfig.nonEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        LOG.info(s"Processing Tag $tagConfig")
        run(spark, tagConfig, trContext)
      }
    }
    spark.close()
  }

  /**
   * Create data source for different data type and run
   *
   * @param session   The Spark Session.
   * @param config    The TagConfigEntry.
   * @param trContext : TrContext
   * @return
   */
  private[this] def run(session: SparkSession,
                        config: TagConfigEntry,
                        trContext: TrContext
                       ): Unit = {
    config.dataSourceConfigEntry.category match {
      case SourceCategory.PARQUET =>
        val parquetConfig = config.dataSourceConfigEntry.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading Parquet files from ${parquetConfig.path}""")
        val reader = new ParquetReader(session, parquetConfig)
        Some(reader.read())

      case SourceCategory.HIVE =>
        val hiveConfig = config.dataSourceConfigEntry.asInstanceOf[HiveSourceConfigEntry]
        val database = config.database
        session.sql(s"use $database")
        LOG.info(s"""Loading from Hive database $database """)
        hiveConfig.tableNames.foreach(tableName => {
          val reader = new HiveReader(session, Array(database, tableName).mkString(".")).read()
          val runner = new HiveFlowStage(reader, config, tableName)
          runner.run_(trContext)
        })
      case _ => {
        LOG.error(s"Data source ${config.dataSourceConfigEntry.category} not supported")
      }
    }
  }
}
