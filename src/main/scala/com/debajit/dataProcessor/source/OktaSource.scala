package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import com.debajit.java.dataProcessor.utils.OktaUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

/**
 * @author debajit
 */

object OktaSource extends Source {

  /**
   * date format for start and end time passed to okta get logs api
   */
  private val publishedTimeFormat: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  val format = new SimpleDateFormat(publishedTimeFormat, Locale.US)
  format.setTimeZone(TimeZone.getTimeZone("UTC"))
  private val spark = SparkHelper.getSparkSession

  /**
   * read okta logs
   *
   * @param sourceProperties source properties
   * @param process          process
   * @param pipeline         pipeline
   * @throws java.lang.Exception exception
   * @return
   */
  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    process match {
      case Some(Fetch.BATCH) => callOktaBatch(sourceProperties)
      case Some(Fetch.REALTIME) => callOktaRealTime(sourceProperties)
      case _ => callOktaRealTime(sourceProperties)
    }
  }

  @throws(classOf[Exception])
  def callOktaBatch(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val schemaLocation = s"${sourceProperties properties "schema_location"}/${sourceProperties properties "schema"}.avsc"
    val schema: StructType = Utils.getStructTypeSchemaFromAvro(schemaLocation)
    val tmpPath = sourceProperties.properties.getOrElse("tmp_storage", "output/okta")
    OktaUtils.getLogs(
      sourceProperties properties "url",
      sourceProperties properties "token",
      format.parse(sourceProperties.properties("start")),
      format.parse(sourceProperties.properties("end")),
      Integer.parseInt(sourceProperties.properties("batch_size")), spark, schema, tmpPath)
    var df = spark.emptyDataFrame
    df = spark.read.parquet(tmpPath)
    (df, None)
  }

  @throws(classOf[Exception])
  def callOktaRealTime(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val df = spark.readStream
      .format("org.apache.spark.sql.execution.streaming.sources.OKTAStreamSourceProvider")
      .options(sourceProperties.properties)
      .load
    (df, None)
  }
}
