package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object BigQuerySource extends Source {
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val resolvedQuery = Utils resolveConfigVariables(sourceProperties properties ("query"), sourceProperties.properties)
    spark.conf.set("credentialsFile", sourceProperties properties "credential_file")
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("parentProject", sourceProperties properties "project_id")
    spark.conf.set("materializationProject", sourceProperties properties "project_id")
    spark.conf.set("materializationDataset", sourceProperties properties "data_set")
    val df = spark.read.format("bigquery")
      .option("credentialsFile", sourceProperties properties "credential_file")
      .load(resolvedQuery)
    (df, None)
  }
}
