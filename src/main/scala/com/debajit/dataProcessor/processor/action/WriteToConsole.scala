package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{ConsoleSink, SinkProperties}
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

class WriteToConsole extends Execute {
  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    ConsoleSink.write(input, SinkProperties(properties))
  }
}
