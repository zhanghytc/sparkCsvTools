package com.tairong.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ServerBaseReader is the abstract class of
 * It include a spark session and a sentence which will sent to service.
 *
 * @param session  Sparksession
 * @param tableName hive table name
 */
abstract class ServerBaseReader(val session: SparkSession, val tableName: String)
  extends Reader {
}

class HiveReader(override val session: SparkSession, override val tableName: String)
  extends ServerBaseReader(session, tableName) {
  override def read(): DataFrame = {
    println("tableName: " + tableName)
    import session.implicits._
    session.sql(s"select * from $tableName")
  }

  override def close(): Unit = {
    session.close()
  }
}
