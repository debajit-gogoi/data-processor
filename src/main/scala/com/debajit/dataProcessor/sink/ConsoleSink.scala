package com.debajit.dataProcessor.sink

import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object ConsoleSink extends Sink {
  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    df.printSchema()
    df.show(numRows = 50, truncate = false)
  }
}
