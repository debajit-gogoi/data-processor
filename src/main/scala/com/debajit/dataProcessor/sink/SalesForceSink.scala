package com.debajit.dataProcessor.sink

import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.{DataFrame, SaveMode}

object SalesForceSink extends Sink {
  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }
    df.write
      .format("com.springml.spark.salesforce")
      .option("login", sinkProperties properties "url")
      .option("username", sinkProperties properties "user")
      .option("password", s"${sinkProperties properties "password"}${sinkProperties properties "security_token"}")
      .option("sfObject", sinkProperties properties "table_name")
      .option("version", "52.0")
      .mode(mode)
      .save()
  }
}
