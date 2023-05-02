package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object RedisSource extends Source {
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    (spark
      .read
      .format("org.apache.spark.sql.redis")
      .options(Utils getRedisClusterReadProperties sourceProperties)
      .option("table", sourceProperties properties "table")
      .option("key.column", sourceProperties properties "key_column")
      .load(), None)
  }

}
