
package com.tairong.common

object SourceCategory extends Enumeration {
  type Type = Value

  val PARQUET = Value("PARQUET")
  val ORC     = Value("ORC")
  val JSON    = Value("JSON")
  val CSV     = Value("CSV")
  val TEXT    = Value("TEXT")
  val HIVE        = Value("HIVE")
}

class SourceCategory

/**
 * DataSourceConfigEntry
 */
sealed trait DataSourceConfigEntry {
  def category: SourceCategory.Value
}

sealed trait FileDataSourceConfigEntry extends DataSourceConfigEntry {
  def path: String
}

sealed trait ServerDataSourceConfigEntry extends DataSourceConfigEntry {
  def tableNames: Array[String]
}

sealed trait StreamingDataSourceConfigEntry extends DataSourceConfigEntry {
  def intervalSeconds: Int
}

/**
 * FileBaseSourceConfigEntry
 *
 * @param category
 * @param path
 * @param separator
 * @param header
 */
case class FileBaseSourceConfigEntry(override val category: SourceCategory.Value,
                                     override val path: String,
                                     separator: Option[String] = None,
                                     header: Option[Boolean] = None)
  extends FileDataSourceConfigEntry {
  override def toString: String = {
    s"File source path: ${path}, separator: ${separator}, header: ${header}"
  }
}

/**
 * HiveSourceConfigEntry
 *
 * @param tableNames
 */
case class HiveSourceConfigEntry(override val category: SourceCategory.Value,
                                 override val tableNames: Array[String])
  extends ServerDataSourceConfigEntry {
  require(tableNames.nonEmpty && tableNames.size > 0 )

  override def toString: String = {
    s"Hive source tables: ${tableNames}"
  }
}









