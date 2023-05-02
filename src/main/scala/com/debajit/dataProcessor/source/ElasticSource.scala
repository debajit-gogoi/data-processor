package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.{DataFrame, DataFrameReader}

/**
 * @author Debajit
 */

object ElasticSource extends Source {

  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val ssl = (sourceProperties.properties contains "ssl") && (sourceProperties properties "ssl" equalsIgnoreCase "true") match {
      case true => "true"
      case _ => "false"
    }
    val metadata = (sourceProperties.properties contains "metadata") && (sourceProperties properties "metadata" equalsIgnoreCase "true") match {
      case true => "true"
      case _ => "false"
    }
    val reader: DataFrameReader = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", metadata)
      .option("es.nodes.wan.only", "true")
      .option("es.port", sourceProperties properties "port")
      .option("es.net.ssl", ssl)
      .option("es.nodes", sourceProperties properties "nodes")
      .option("pushdown", "true")
    (setAdditionalParams(reader, sourceProperties) load (sourceProperties properties "index"), None)
  }

  def setAdditionalParams(reader: DataFrameReader, sourceProperties: SourceProperties): DataFrameReader = {
    val readerWithQuery: DataFrameReader = if (sourceProperties.properties contains "query") reader option("es.query", sourceProperties properties "query") else reader
    if ((sourceProperties.properties contains "user") && (sourceProperties.properties contains "password"))
      readerWithQuery option("es.net.http.auth.user", sourceProperties properties "user") option("es.net.http.auth.pass", sourceProperties properties "password")
    else readerWithQuery
  }
}
