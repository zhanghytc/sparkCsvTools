package com.tairong.common

import com.tairong.common.TrRunEnum.local
import org.apache.log4j.Logger
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object TrSparkConf {
	private[this] val LOG = Logger.getLogger(this.getClass)

	var sparkConf = new SparkConf()

	def createSparkContext(appName: String, configs:Configs) = {
		sparkConf.setAppName(appName)
		sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)

		if (configs.sysConfEntity.power.equals(local.toString)) { // 运行模式的开关
			sparkConf.setMaster("local[*]")
		}
		// 封装用户传递进来的参数
//		configs.sparkConfigEntry.map { case (key:String, value:Any) => sparkConf.set(key, value.toString) }

		new SparkContext(sparkConf)
	}

	def createSparkSession(c: Argument,configs: Configs,programName:String): SparkSession = {

		val session = SparkSession
			.builder()
			.appName(programName)
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.config("spark.sql.shuffle.partitions", "1")

		val _power = configs.sysConfEntity.power
		if(_power.equals(local.toString)) {
			session.master("local[*]")
		} else if(_power.equalsIgnoreCase("cluster")) {
			session.master("yarn-cluster")
		}

		// config hive for sparkSession
		if (c.hive) {
			if (configs.hiveConfigEntry.isEmpty) {
				LOG.info("you don't config hive source, so using hive tied with spark.")
			} else {
				val hiveConfig = configs.hiveConfigEntry.get
				sparkConf.set("spark.sql.warehouse.dir", hiveConfig.warehouse)
				sparkConf
					.set("javax.jdo.option.ConnectionURL", hiveConfig.connectionURL)
					.set("javax.jdo.option.ConnectionDriverName", hiveConfig.connectionDriverName)
					.set("javax.jdo.option.ConnectionUserName", hiveConfig.connectionUserName)
					.set("javax.jdo.option.ConnectionPassword", hiveConfig.connectionPassWord)
			}
		}
		session.config(sparkConf)
		if (c.hive) {
			session.enableHiveSupport()
		}
		session.getOrCreate()
	}
}
