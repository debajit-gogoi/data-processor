package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.DataFrame

import java.util.Properties

/**
 * @author debajit
 */
object SnowFlakeSource extends Source {
  private val spark = SparkHelper.getSparkSession

  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val props = new Properties()
    props.put("driver","net.snowflake.client.jdbc.SnowflakeDriver")
    props.put("user", sourceProperties properties "user")
    props.put("password", sourceProperties properties "password")
    props.put("warehouse", sourceProperties properties "warehouse")
    props.put("db", sourceProperties properties "db_name")
    props.put("schema", sourceProperties properties "schema")
    val df = spark.read.jdbc(sourceProperties properties "url", sourceProperties properties "query", props)
    (df, None)
  }
}
