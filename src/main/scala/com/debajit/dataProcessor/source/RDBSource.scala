package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.DataFrame

import java.util.Properties

/**
 * @author Debajit
 */

object RDBSource extends Source {
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val url = sourceProperties.properties("url")

    val df = spark.read.format("jdbc")
      .option("url",url)
      .option("driver",sourceProperties properties "driver")
      .option("user", sourceProperties properties "user")
      .option("password", sourceProperties properties "password")
      .option("query",sourceProperties properties "query").load()
       (df, None)
  }
}
