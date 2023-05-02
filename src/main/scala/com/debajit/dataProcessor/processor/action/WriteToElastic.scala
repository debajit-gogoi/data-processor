package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{ElasticSink, SinkProperties}
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

class WriteToElastic extends Execute {
  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    ElasticSink write(df = input,
      sinkProperties = SinkProperties(properties = properties))
  }
}
