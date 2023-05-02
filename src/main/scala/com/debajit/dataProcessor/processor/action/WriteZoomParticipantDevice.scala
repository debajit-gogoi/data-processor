package com.debajit.dataProcessor.processor.action

import com.debajit.dataProcessor.processor.Execute
import com.debajit.dataProcessor.sink.{SinkProperties, ZoomParticipantSink}
import org.apache.spark.sql.DataFrame

private class WriteZoomParticipantDevice extends Execute {

  override def write(input: DataFrame, batchId: Option[Long], properties: Map[String, String]): Unit = {
    ZoomParticipantSink.write(input, SinkProperties(properties))
  }
}
