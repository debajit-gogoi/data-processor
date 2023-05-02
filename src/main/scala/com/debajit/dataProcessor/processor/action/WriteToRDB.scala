package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{RDBSink, SinkProperties}
import org.apache.spark.sql.DataFrame

class WriteToRDB extends Execute{

  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    RDBSink write(df = input,
      sinkProperties = SinkProperties(properties = properties))
  }

}
