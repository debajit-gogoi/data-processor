package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{KafkaSink, SinkProperties}
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

class WriteToKafka extends Execute {
  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    KafkaSink write(df = input,
      sinkProperties = SinkProperties(properties = properties))
  }
}
