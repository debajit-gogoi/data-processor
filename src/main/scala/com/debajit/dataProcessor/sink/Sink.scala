package com.debajit.dataProcessor.sink

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

trait Sink extends LazyLogging {
  @throws(classOf[Exception])
  def write(df: DataFrame, sinkProperties: SinkProperties): Unit
}

case class SinkProperties(properties: Map[String, String])
