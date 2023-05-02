package com.debajit.dataProcessor.sink

import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util
import scala.util.{Failure, Success, Try}

object ElasticSink extends Sink {

  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    Try{
      saveToElastic(df, sinkProperties)
    }
    match {
      case Success(_) =>
      case Failure(exception) =>
        println(exception.printStackTrace())
        Thread.sleep(2000)
        println("Retrying writing to Elastic !! ")
        Try{
          saveToElastic(df, sinkProperties)
        }
        match {
          case Success(_) =>
          case Failure(exception) =>
            println(exception.printStackTrace())
            throw new RuntimeException()
        }
    }
  }

  def saveToElastic(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }

    val index = (sinkProperties.properties contains "ts_col") && (sinkProperties.properties contains "ts_format") match {
      case true => Seq(sinkProperties properties "index",
        Seq(sinkProperties.properties("ts_col"),sinkProperties.properties("ts_format")).mkString("{","|","}")) mkString "-"
      case false => sinkProperties properties "index"
    }
    df.write.format("org.elasticsearch.spark.sql")
      .options(getESConf(sinkProperties))
      .mode(mode)
      .option("es.resource", index)
      .save()
  }

  def getESConf(sinkProperties: SinkProperties): util.HashMap[String, String] = {
    val esConf: util.HashMap[String, String] = new util.HashMap[String, String]()
    if (sinkProperties.properties contains "mapping_id") esConf put("es.mapping.id", sinkProperties properties "mapping_id")
    if (sinkProperties.properties contains "pipeline") esConf put("es.ingest.pipeline", "geoip")
    if ((sinkProperties.properties contains "upsert") && (sinkProperties properties "upsert" equalsIgnoreCase "upsert"))
      esConf put("es.write.operation", "upsert")

    (sinkProperties.properties contains "ssl") && (sinkProperties properties "ssl" equalsIgnoreCase "true") match {
      case true => esConf put("es.net.ssl", "true")
      case _ => esConf put("es.net.ssl", "false")
    }

    esConf put("es.index.auto.create", "true")
    esConf put("es.port", sinkProperties properties "port")
    esConf put("es.nodes", sinkProperties properties "nodes")

    if (sinkProperties.properties contains "retryOnConflict")
      esConf put("es.update.retry.on.conflict", sinkProperties properties "retryOnConflict")
    if ((sinkProperties.properties contains "wan_only") && (sinkProperties properties "wan_only" equalsIgnoreCase "true"))
      esConf put("es.nodes.wan.only", "true")
    if ((sinkProperties.properties contains "data_only") && (sinkProperties properties "data_only" equalsIgnoreCase "true"))
      esConf put("es.nodes.data.only", "true")
    else
      esConf put("es.nodes.data.only", "false")
    if (sinkProperties.properties contains "user")
      esConf put("es.net.http.auth.user", sinkProperties properties "user")
    if (sinkProperties.properties contains "password")
      esConf put("es.net.http.auth.pass", sinkProperties properties "password")

    if (sinkProperties.properties contains "retryCount")
      esConf put("es.batch.write.retry.count", sinkProperties properties "retryCount")
    if (sinkProperties.properties contains "retryWaitTime")
      esConf put("es.batch.write.retry.wait", sinkProperties properties "retryWaitTime")

    esConf
  }
}
