package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.SalesforceFetchUtils
import org.apache.spark.sql.DataFrame

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}

/**
 * @author Debajit
 */

object SalesForceSource extends Source {

  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    process match {
      case Some(Fetch.BATCH) => callSalesForceReadBatch(sourceProperties)
      case Some(Fetch.REALTIME) => callSalesForceReadRealtime(sourceProperties)
      case _ => callSalesForceReadRealtime(sourceProperties)
    }
  }

  @throws(classOf[Exception])
  def callSalesForceReadRealtime(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val sfDF = spark.readStream
      .format("org.apache.spark.sql.execution.streaming.sources.SFDCStreamSourceProvider")
      .options(sourceProperties.properties)
      .load
    (sfDF, None)
  }

  @throws(classOf[Exception])
  def callSalesForceReadBatch(sourceProperties: SourceProperties): (DataFrame, Option[Any]) = {
    val sfDF = sourceProperties.properties.contains("last_n_days") match {
      case true => val noOfDays = Integer parseInt (sourceProperties properties "last_n_days")
        println(s"no of days is $noOfDays")
        val dateTimeToStart: LocalDateTime = LocalDateTime of(LocalDate now ZoneOffset.UTC minusDays noOfDays, LocalTime.MIDNIGHT)
        val startTime = sourceProperties properties "type_of_extraction" match {
          case "soql" => dateTimeToStart format (DateTimeFormatter ofPattern SalesforceFetchUtils.SystemModStampTimeFormatSoql)
          case "cdata" => dateTimeToStart format (DateTimeFormatter ofPattern SalesforceFetchUtils.SystemModStampTimeFormatJDBC)
        }
        println(s"start time is $startTime")
        SalesforceFetchUtils fetchSFUtil(spark = spark, sourceProperties.properties, Some(startTime), None)
      case false => SalesforceFetchUtils fetchSFUtil(spark = spark, sourceProperties.properties,
        startTime = if (sourceProperties.properties.contains("start_time")) Some(sourceProperties properties "start_time") else None,
        endTime = if (sourceProperties.properties.contains("end_time")) Some(sourceProperties properties "end_time") else None
      )
    }
    (sfDF, None)
  }
}
