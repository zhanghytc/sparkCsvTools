
package com.tairong.common

sealed trait SchemaConfigEntry {


  /** see{@link DataSourceConfigEntry}*/
  def dataSourceConfigEntry: DataSourceConfigEntry

  /** see{@link DataSinkConfigEntry}*/
  def dataSinkConfigEntry: DataSinkConfigEntry

  /** spark partition */
  def partition: Int
}

case class TagConfigEntry(
                           override val dataSourceConfigEntry: DataSourceConfigEntry,
                           override val dataSinkConfigEntry: DataSinkConfigEntry,
                           database:String,
                           override val partition: Int
                         )
  extends SchemaConfigEntry {

  override def toString: String = {
    s"source: $dataSourceConfigEntry, " +
      s"sink: $dataSinkConfigEntry, " +
      s"partition: $partition."

  }
}
