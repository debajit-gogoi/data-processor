package com.debajit.dataProcessor.source

import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

trait Source {
  @throws(classOf[Exception])
  def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any])
}

case class SourceProperties(properties: Map[String, String])