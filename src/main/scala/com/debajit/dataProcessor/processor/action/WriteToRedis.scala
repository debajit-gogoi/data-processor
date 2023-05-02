package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{RedisSink, SinkProperties}
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

class WriteToRedis extends Execute {
  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    RedisSink write(df = input,
      sinkProperties = SinkProperties(properties))
  }
}
