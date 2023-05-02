package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import com.debajit.java.dataProcessor.utils.PeopleAIUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * @author debajit
 */
object PeopleAISource extends Source {
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val url = sourceProperties.properties("url")
    val clientId = sourceProperties.properties("client_id")
    val clientSecret = sourceProperties.properties("client_secret")
    val tmpPath = sourceProperties.properties.getOrElse("tmp_storage", "output/peopleai")
    val startDate = sourceProperties.properties("start_date")
    val endDate = sourceProperties.properties("end_date")
    val exportType = sourceProperties.properties.getOrElse("export_type", "Delta")
    val batchSize = sourceProperties.properties("batch_size")
    val schemaLocation = s"${sourceProperties properties "schema_location"}/${sourceProperties properties "schema"}.avsc"
    val schema: StructType = Utils.getStructTypeSchemaFromAvro(schemaLocation)
    PeopleAIUtils.getPeopleAIDate(spark, url, clientId, clientSecret, tmpPath, startDate, endDate, exportType, batchSize.toInt, schema)
    var df = spark.emptyDataFrame
    df = spark.read.parquet(tmpPath)
    (df, None)
  }
}
