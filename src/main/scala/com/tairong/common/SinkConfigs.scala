/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.tairong.common

/**
  * SinkCategory is used to expression the writer's type.
  */
object SinkCategory extends Enumeration {
  type Type = Value

  val HIVE = Value("hive")
  val CSV    = Value("csv")
}


class SinkCategory

/**
 * DataSinkConfigEntry
 */
sealed trait DataSinkConfigEntry {
  def category: SinkCategory.Value
}

/**
 * FileBaseSinkConfigEntry
 */
case class HiveSinkConfigEntry(override val category: SinkCategory.Value,
                                   tableName: String
                                  )
  extends DataSinkConfigEntry {
  override def toString: String = {
    s"File sink:  to ${tableName}"
  }
}

/**
 * FileBaseSinkConfigEntry
 */
case class FileBaseSinkConfigEntry(override val category: SinkCategory.Value,
                                   srcPath: String,
                                   targetPath: String,
                                   fsName: Option[String]
                                   )
  extends DataSinkConfigEntry {
  override def toString: String = {
    s"File sink: from ${fsName.get}${srcPath} to ${targetPath} "
//    s"File sink: to ${targetPath} "
  }
}

/**
 * TrSinkConfigEntry use to specified the sink service's path.
 */
case class TrSinkConfigEntry(override val category: SinkCategory.Value, targetPath: String)
  extends DataSinkConfigEntry {
  override def toString: String = {
    s"csv sink targetPath: $targetPath)}"
  }
}

