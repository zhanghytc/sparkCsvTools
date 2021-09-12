package com.tairong.common

import java.io.File
import java.nio.file.Files
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.break

case class TrContext(spark: SparkSession, conf: Configs)

case class Configs(sysConfEntity: SysConfEntity,
                   tagsConfig: mutable.ListBuffer[TagConfigEntry],
                   sparkConfigEntry: SparkConfigEntry,
                   hiveConfigEntry: Option[HiveConfigEntry] = None)

case class SysConfEntity(power: String) {
  require(power.equalsIgnoreCase("local") || power.equalsIgnoreCase("cluster"), "power must be local or cluster")

  override def toString: String = super.toString
}


case class SparkConfigEntry(map: Map[String, Any]) {
  override def toString: String = {
    ""
  }
}

final case class Argument(config: String = "application.conf",
                          hive: Boolean = false,
                          directly: Boolean = false,
                          dry: Boolean = false,
                          reload: String = "")


case class HiveConfigEntry(warehouse: String,
                           connectionURL: String,
                           connectionDriverName: String,
                           connectionUserName: String,
                           connectionPassWord: String) {
  override def toString: String =
    s"HiveConfigEntry:{" +
      s"warehouse=$warehouse, " +
      s"connectionURL=$connectionURL, " +
      s"connectionDriverName=$connectionDriverName, " +
      s"connectionUserName=$connectionUserName, " +
      s"connectionPassWord=$connectionPassWord}"
}



object SparkConfigEntry {
  def apply(config: Config): SparkConfigEntry = {
    val map = mutable.Map[String, String]()
    val sparkConfig = config.getObject("spark")
    for (key <- sparkConfig.unwrapped().keySet().asScala) {
      val sparkKey = s"spark.$key"
      if (config.getAnyRef(sparkKey).isInstanceOf[String]) {
        val sparkValue = config.getString(sparkKey)
        map += sparkKey -> sparkValue
      } else {
        for (subKey <- config.getObject(sparkKey).unwrapped().keySet().asScala) {
          val key = s"$sparkKey.$subKey"
          val sparkValue = config.getString(key)
          map += key -> sparkValue
        }
      }
    }
    SparkConfigEntry(map.toMap)
  }
}

object TrConfig {

  private[this] val LOG = Logger.getLogger(this.getClass)

  private[this] val DEFAULT_PARTITION            = -1


  def parse(configPath: File): Configs = {

    if (!Files.exists(configPath.toPath)) {
      throw new IllegalArgumentException(s"$configPath not exist")
    }

    val config = ConfigFactory.parseFile(configPath)

    val _power = config.getString("trconfig.common.power")
    val sysConfEntity = SysConfEntity(_power)

    val tags = mutable.ListBuffer[TagConfigEntry]()
    val tagConfigs: Option[util.List[_ <: Config]] = getConfigsOrNone(config, "tags")
    println(s"tagConfigs:$tagConfigs")
    if (tagConfigs.isDefined) {
      for (tagConfig <- tagConfigs.get.asScala) {
        println(s"tagConfig:" + tagConfig)
        if (!tagConfig.hasPath("name") ||
          !tagConfig.hasPath("type.source")) {
          println("The `name` and `type` must be specified")
          LOG.error("The `name` and `type` must be specified")
          break()
        }

        // TODO 生成 TagConfigEntry
        val sourceCategory = toSourceCategory(tagConfig.getString("type.source"))
        val sourceConfig = dataSourceConfig(sourceCategory, tagConfig)


        val sinkCategory = toSinkCategory(tagConfig.getString("type.sink"))
        val sinkConfig   = dataSinkConfig(sinkCategory, tagConfig)
        val database = tagConfig.getString("database")
        val partition = getOrElse(tagConfig, "partition", DEFAULT_PARTITION)


        val entry = TagConfigEntry(
          sourceConfig,
          sinkConfig,
          database,
          partition
        )
        LOG.info(s"Tag Config: $entry")
        tags += entry
      }
    }
    val sparkConfigEntry = SparkConfigEntry(config)
    Configs(sysConfEntity, tags, sparkConfigEntry)
  }


  /**
   * Get the value from config by the path. If the path not exist, return the default value.
   *
   * @param config       The config.
   * @param path         The path of the config.
   * @param defaultValue The default value for the path.
   * @return
   */
  private[this] def getOrElse[T](config: Config, path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      config.getAnyRef(path).asInstanceOf[T]
    } else {
      defaultValue
    }
  }


  private[this] def getConfigsOrNone(config: Config,
                                     path: String): Option[java.util.List[_ <: Config]] = {
    if (config.hasPath(path)) {
      Some(config.getConfigList(path))
    } else {
      None
    }
  }


  private[this] def toSourceCategory(category: String): SourceCategory.Value = {
    category.trim.toUpperCase match {
      case "PARQUET" => SourceCategory.PARQUET
      case "ORC" => SourceCategory.ORC
      case "JSON" => SourceCategory.JSON
      case "CSV" => SourceCategory.CSV
      case "HIVE" => SourceCategory.HIVE
      case _ => throw new IllegalArgumentException(s"$category not support")
    }
  }


  private[this] def dataSourceConfig(category: SourceCategory.Value,
                                     config: Config
                                    ): DataSourceConfigEntry = {
    category match {
      case SourceCategory.PARQUET =>
        FileBaseSourceConfigEntry(SourceCategory.PARQUET, config.getString("path"))
      case SourceCategory.ORC =>
        FileBaseSourceConfigEntry(SourceCategory.ORC, config.getString("path"))
      case SourceCategory.JSON =>
        FileBaseSourceConfigEntry(SourceCategory.JSON, config.getString("path"))
      case SourceCategory.CSV =>
        val separator =
          if (config.hasPath("separator"))
            config.getString("separator")
          else ","
        val header =
          if (config.hasPath("header"))
            config.getBoolean("header")
          else
            false
        FileBaseSourceConfigEntry(SourceCategory.CSV,
          config.getString("path"),
          Some(separator),
          Some(header))
      case SourceCategory.HIVE =>
        HiveSourceConfigEntry(SourceCategory.HIVE, config.getString("tables").split(","))

      case _ =>
        throw new IllegalArgumentException("Unsupported data source")
    }
  }


  /**
   * Use to sink name to sink value mapping.
   *
   * @param category name
   * @return
   */
  private[this] def toSinkCategory(category: String): SinkCategory.Value = {
    category.trim.toUpperCase match {
      case "HIVE" => SinkCategory.HIVE
      case "CSV"    => SinkCategory.CSV
      case _        => throw new IllegalArgumentException(s"${category} not support")
    }
  }

  private[this] def dataSinkConfig(category: SinkCategory.Value,
                                   config: Config): DataSinkConfigEntry = {
    category match {
      case SinkCategory.HIVE =>
        TrSinkConfigEntry(SinkCategory.HIVE,
          config.getString("tableName"))
      case SinkCategory.CSV => {
        val fsNameNode = {
          if (config.hasPath("sink.hdfs.namenode"))
            Option(config.getString("sink.hdfs.namenode"))
          else {
            LOG.warn("csv save path hdfs namenode is not set.")
            Option.empty
          }
        }

        FileBaseSinkConfigEntry(SinkCategory.CSV,
          config.getString("sink.srcPath"),
          config.getString("sink.targetPath"),
          fsNameNode
          )
      }
      case _ =>
        throw new IllegalArgumentException("Unsupported data sink")
    }
  }

  /**
   * Use to parse command line arguments.
   *
   * @param args
   * @param programName
   * @return Argument
   */
  def parser(args: Array[String], programName: String): Option[Argument] = {
    val parser = new scopt.OptionParser[Argument](programName) {
      head(programName, "1.0.0")

      opt[String]('c', "config")
        .required()
        .valueName("fileName")
        .action((x, c) => c.copy(config = x))
        .text("config fileName")

      opt[Unit]('h', "hive")
        .action((_, c) => c.copy(hive = true))
        .text("hive supported")

      opt[String]('r', "reload")
        .valueName("<path>")
        .action((x, c) => c.copy(reload = x))
        .text("reload path")
    }
    parser.parse(args, Argument())
  }

}
