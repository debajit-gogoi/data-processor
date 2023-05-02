package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, DataFrameReader}

/**
 * @author Debajit
 */

object FileSource extends Source {

  /**
   * spark session
   */
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    if (sourceProperties.properties.getOrElse("source", "filesystem") == "s3") {
      Utils fileReadS3PreConfig(spark, sourceProperties)
    }
    process match {
      case Some(Fetch.BATCH) => callFileReadBatch(sourceProperties)
      case Some(Fetch.REALTIME) => callFileReadRealtime(sourceProperties)
      case _ => callFileReadRealtime(sourceProperties)
    }
  }

  def callFileReadBatch(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val format: String = (Utils getFileFormat sourceProperties).toLowerCase
    val dataFrameReader: DataFrameReader = format match {
      case "csv" =>
        val dfReader = spark.read options (Utils getCSVSpecificBatchConfigs sourceProperties) format format
        sourceProperties.properties.contains("schema_location") && sourceProperties.properties.contains("schema") match {
          case true => val schemaLocation = s"${sourceProperties properties "schema_location"}/${sourceProperties properties "schema"}.avsc"
            dfReader schema (Utils getStructTypeSchemaFromAvro schemaLocation)
          case _ => dfReader
        }
      case _ => spark.read format format
    }
    val df = dataFrameReader
      .load(sourceProperties.properties.getOrElse("path", "input"))
    (df, None)
  }

  def callFileReadRealtime(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val format: String = (Utils getFileFormat sourceProperties).toLowerCase
    val dataStreamReader: DataStreamReader = format match {
      case "csv" =>
        val dfReader = spark.readStream options (Utils getCSVSpecificBatchConfigs sourceProperties) format format
        sourceProperties.properties.contains("schema_location") && sourceProperties.properties.contains("schema") match {
          case true => val schemaLocation = s"${sourceProperties properties "schema_location"}/${sourceProperties properties "schema"}.avsc"
            dfReader schema (Utils getStructTypeSchemaFromAvro schemaLocation)
          case _ => dfReader
        }
      case _ => spark.readStream format format
    }
    val df = dataStreamReader
      .option("maxFilesPerTrigger", sourceProperties properties "max_files_per_trigger")
      .load(sourceProperties.properties.getOrElse("path", "input"))
    (df, None)
  }
}
