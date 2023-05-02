package com.debajit.dataProcessor.sink

import com.debajit.dataProcessor.utils.Utils
import com.debajit.dataProcessor.utils.Utils.getColsSeqString
import org.apache.spark.sql._
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.functions.{col, concat_ws, struct, to_json}

import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * @author Debajit
 */

object KafkaSink extends Sink {
  /**
   * write data to kafka
   *
   * @param df             data frame
   * @param sinkProperties sink properties
   */

  @throws(classOf[Exception])
  def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val format = "kafka"
    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }
    val location = sinkProperties.properties("topic")
    val bootstrap_servers = sinkProperties.properties("bootstrap_servers")

    val data_format = sinkProperties.properties.contains("data_format") match {
      case true => sinkProperties properties "data_format"
      case false => "json"
    }

    Try {
      val toWrite: DataFrameWriter[Row] = data_format match {
        case "json" =>
          if (sinkProperties.properties contains "kafka_partition_cols") {
            val partitionCols = getColsSeqString(sinkProperties.properties get "kafka_partition_cols")
            val partitionColsKafka: Seq[Column] = partitionCols.get map (column => col(column))
            df.select(concat_ws(":", partitionColsKafka: _*) as "key", to_json(struct("*")) as "value").write
          } else df.select(to_json(struct("*")) as "value").write
        case "text" =>
          if (sinkProperties.properties contains "kafka_partition_cols") {
            val partitionCols = getColsSeqString(sinkProperties.properties get "kafka_partition_cols")
            val partitionColsKafka: Seq[Column] = partitionCols.get map (column => col(column))
            df.select(concat_ws(":", partitionColsKafka: _*) as "key", col("value") as "value").write
          } else df.select(col("key") as "key", col("value") as "value").write
        case "avro" =>
          if (sinkProperties.properties contains "kafka_partition_cols") {
            val partitionCols = getColsSeqString(sinkProperties.properties get "kafka_partition_cols")
            val partitionColsKafka: Seq[Column] = partitionCols.get map (column => col(column))
            df.select(concat_ws(":", partitionColsKafka: _*) as "key", to_avro(struct("*")) as "value").write
          } else df.select(to_avro(struct("*")) as "value").write
        case "json_connect" =>
          val kafkaConnectPath = sinkProperties.properties("kafka_connect_schema_path")
          val kafkaConnectSchema = sinkProperties.properties("kafka_connect_schema")
          val schemaLocation = s"$kafkaConnectPath${File.separator}$kafkaConnectSchema.json"
          val schemaAsString = Utils.readConfigFileAsString(schemaLocation)
          import df.sparkSession.implicits._
          val dfSchema = df.sparkSession.read.json(Seq(schemaAsString).toDS).select(struct("*") as "schema")
          val dfWithSchema = df.select(df.columns.map(x => col(x).as(x.toLowerCase)): _*).withColumn("payload", struct("*")).crossJoin(dfSchema)
          if (sinkProperties.properties contains "kafka_partition_cols") {
            val partitionCols = getColsSeqString(sinkProperties.properties get "kafka_partition_cols")
            val partitionColsKafka: Seq[Column] = partitionCols.get map (column => col(column))
            dfWithSchema.select(concat_ws(":", partitionColsKafka: _*) as "key", to_json(struct("schema", "payload")) as "value").write
          } else dfWithSchema.select(to_json(struct("schema", "payload")) as "value").write
      }

      val dataFrameWriter = toWrite
        .format(format)
        .mode(mode)
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", location)

      setJasConfig(dataFrameWriter, sinkProperties) save()
    }
    match {
      case Success(_) =>
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def setJasConfig(dataFrameWriter: DataFrameWriter[Row], sinkProperties: SinkProperties): DataFrameWriter[Row] = {
    Try {
      if ((sinkProperties.properties contains "is_ssL_enabled") && (sinkProperties properties "is_ssL_enabled" equalsIgnoreCase "true")) {
        val userName = sinkProperties properties "user"
        val password = sinkProperties properties "password"
        val jasConfig: String = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$userName" password="$password";"""
        dataFrameWriter
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.sasl.jaas.config", jasConfig)
      }
      else dataFrameWriter
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }
}
