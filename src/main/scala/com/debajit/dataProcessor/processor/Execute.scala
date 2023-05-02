package com.debajit.dataProcessor.processor

import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

trait Execute {
  def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit
}
