package com.debajit.dataProcessor.sink

import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author Debajit
 */

object RedisSink extends Sink {
  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }
    df.write
      .format("org.apache.spark.sql.redis")
      .options(Utils getRedisClusterWriteProperties sinkProperties)
      .option("table", sinkProperties properties "table")
      .option("key.column", sinkProperties properties "key_column")
      .mode(mode)
      .save()
  }
}
