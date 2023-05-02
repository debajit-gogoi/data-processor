package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils.ssc
import com.debajit.dataProcessor.utils.{KafkaHelper, Utils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * @author Debajit
 */

object KafkaSource extends Source with LazyLogging {

  /**
   * spark session
   */
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val dataFormat: String = sourceProperties.properties.contains("data_format") match {
      case true => sourceProperties properties "data_format"
      case false => "json"

    }
    val startingOffset: String = sourceProperties.properties.contains("startingOffsets") match {
      case true => sourceProperties.properties("startingOffsets")
      case false => "earliest"
    }

    val filterValue: Option[String] = sourceProperties.properties.contains("filter_value") match {
      case true => Some(sourceProperties.properties("filter_value"))
      case _ => None
    }
    Try {
      process match {
        case Some(Fetch.BATCH) => callKafkaReadBatch(sourceProperties, dataFormat, startingOffset, filterValue)
        case Some(Fetch.REALTIME) => callKafkaReadRealTime(sourceProperties, dataFormat, startingOffset, filterValue)
        case _ => callKafkaReadRealTime(sourceProperties, dataFormat, startingOffset, filterValue)
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def callKafkaReadRealTime(sourceProperties: SourceProperties, dataFormat: String, startingOffset: String, filterValue: Option[String]): (DataFrame, Option[Any]) = {
    val df = Try {
      dataFormat match {
        case "json" =>
          val schemaLocation = s"${sourceProperties properties "schema_location"}${File.separator}${sourceProperties properties "schema"}.avsc"
          readJsonAsStringFromKafkaRealTime(
            schema_location = schemaLocation,
            partitionsAndOffsets = startingOffset,
            filterValue = filterValue,
            sourceProperties = sourceProperties
          )
        case "avro" =>
          val schemaLocation = s"${sourceProperties properties "schema_location"}${File.separator}${sourceProperties properties "schema"}.avsc"
          readAvroFromKafkaRealTime(
            schema_location = schemaLocation,
            partitionsAndOffsets = startingOffset,
            filterValue = filterValue,
            sourceProperties = sourceProperties
          )
        case "text" =>
          readTextFromKafkaRealTime(
            partitionsAndOffsets = startingOffset,
            filterValue = filterValue,
            sourceProperties = sourceProperties)
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
    (df, None)
  }

  @throws(classOf[Exception])
  def callKafkaReadBatch(sourceProperties: SourceProperties, dataFormat: String, startingOffset: String, filterValue: Option[String]): (DataFrame, Option[Any]) = {
    val groupId = sourceProperties properties "group_id"
    val currentKafkaConsumer = KafkaHelper kafkaConsumer(groupId, sourceProperties properties "bootstrap_servers")
    val topic = sourceProperties properties "topic"
    val startingOffsets = KafkaHelper getStartingOffsetsString(currentKafkaConsumer, topic)

    val df = Try {
      dataFormat match {
        case "json" =>
          val schemaLocation = s"${sourceProperties properties "schema_location"}${File.separator}${sourceProperties properties "schema"}.avsc"
          readJsonAsStringFromKafkaBatch(
            startingOption = startingOffsets,
            schema_location = schemaLocation,
            filterValue = filterValue,
            sourceProperties = sourceProperties
          )
        case "avro" =>
          val schemaLocation = s"${sourceProperties properties "schema_location"}${File.separator}${sourceProperties properties "schema"}.avsc"
          readAvroFromKafkaBatch(
            startingOption = startingOffsets,
            schema_location = schemaLocation,
            filterValue = filterValue,
            sourceProperties = sourceProperties
          )
        case "text" =>
          readTextFromKafkaBatch(
          startingOption = startingOffsets,
          filterValue = filterValue,
          sourceProperties = sourceProperties)
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }


    (df, Some(currentKafkaConsumer))
  }

  @throws(classOf[Exception])
  def getInitialDataStreamReader(sourceProperties: SourceProperties, startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest"): DataStreamReader ={
   Try{
     spark
       .readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", sourceProperties properties "bootstrap_servers")
       .option("enable.auto.commit", value = false)
       .option("group.id", sourceProperties properties "group_id")
       .option("subscribe", sourceProperties properties "topic")
       .option("failOnDataLoss", value = false)
       .option(startingOption, partitionsAndOffsets)
       .option("maxOffsetsPerTrigger", (sourceProperties properties "offsets_per_trigger").toInt)
   }
   match {
     case Success(value) => value
     case Failure(exception) => println(exception.printStackTrace())
       throw new RuntimeException()
   }
  }

  @throws(classOf[Exception])
  def getInitialDataFrameReader(sourceProperties: SourceProperties, startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest"): DataFrameReader = {
    Try{
      spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", sourceProperties properties "bootstrap_servers")
        .option("subscribe", sourceProperties properties "topic")
        .option("enable.auto.commit", value = false)
        .option("group.id", sourceProperties properties "group_id")
        .option("failOnDataLoss", value = false)
        .option(startingOption, partitionsAndOffsets)
        .option("endingOffsets", "latest")
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readTextFromKafkaBatch(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                             filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {

    Try {
      val dataFrameReader: DataFrameReader =
        getInitialDataFrameReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df = setJasConfigBatch(dataFrameReader, sourceProperties)
        .load()

      filterKeyText(df, filterValue)
    } match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readJsonAsStringFromKafkaBatch(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                                     schema_location: String,filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {

    Try {
      val dataFrameReader: DataFrameReader =
        getInitialDataFrameReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df = setJasConfigBatch(dataFrameReader, sourceProperties)
        .load()
      filterKeyJson(df, filterValue, schema_location)
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readAvroFromKafkaBatch(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                             schema_location: String, filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {

    Try {
      val dataFrameReader: DataFrameReader =
        getInitialDataFrameReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df = setJasConfigBatch(dataFrameReader, sourceProperties)
        .load()
      filterKeyAvro(df, filterValue, schema_location)
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readTextFromKafkaRealTime(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                                filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {
    Try {
      val dataStreamReader: DataStreamReader =
        getInitialDataStreamReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df: DataFrame = setJasConfigStream(dataStreamReader, sourceProperties)
        .load()
      filterKeyText(df, filterValue)
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readAvroFromKafkaRealTime(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                                schema_location: String, filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {
    Try {
      val dataStreamReader: DataStreamReader =
        getInitialDataStreamReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df = setJasConfigStream(dataStreamReader, sourceProperties)
        .load()

      filterKeyAvro(df, filterValue, schema_location)
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readJsonAsStringFromKafkaRealTime(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest",
                                        schema_location: String, filterValue: Option[String], sourceProperties: SourceProperties): DataFrame = {
    Try {
      val dataStreamReader: DataStreamReader =
        getInitialDataStreamReader(sourceProperties, startingOption, partitionsAndOffsets)

      val df = setJasConfigStream(dataStreamReader, sourceProperties)
        .load()

      filterKeyJson(df, filterValue, schema_location)
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def filterKeyAvro(df: DataFrame, filterValue: Option[String], schemaLocation: String): DataFrame = {
    Try {
      filterValue match {
        case None =>
          df select (from_avro(col("value"), Utils readConfigFileAsString  schemaLocation) as "data") select "data.*"
        case _ =>
          val filterVal: String = filterValue.get
          df.select(df.col("*")).filter(col("key").like(s"%$filterVal%")).select(from_avro(col("value"), Utils readConfigFileAsString  schemaLocation) as "data").select("data.*")
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def filterKeyText(df: DataFrame, filterValue: Option[String]): DataFrame = {
    Try {
      filterValue match {
        case None =>
          df selectExpr "CAST(value AS STRING)"
        case _ =>
          val filterVal = filterValue.get
          df.select(df.col("*")).filter(col("key").like(s"%$filterVal%")).selectExpr("CAST(value AS STRING)")
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def filterKeyJson(df: DataFrame, filterValue: Option[String], schemaLocation: String): DataFrame = {
    Try {
      filterValue match {
        case None =>
          df selectExpr "CAST(value AS STRING)" select (from_json(col("value"), Utils getStructTypeSchemaFromAvro schemaLocation) as "data") select "data.*"
        case _ =>
          val filterVal: String = filterValue.get
          df.select(df.col("*")).filter(col("key").like(s"%$filterVal%")).selectExpr("CAST(value AS STRING)").select(from_json(col("value"), Utils.getStructTypeSchemaFromAvro(schemaLocation)) as "data").select("data.*")
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def readJsonAsStringFromKafkaRealTimeGroup(partitionsAndOffsets: String = "earliest", topic: String, groupId: String, bootstrap_servers: String): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> partitionsAndOffsets,
      "enable.auto.commit" -> ("false".toBoolean: java.lang.Boolean)
    )
    val topics = Array(topic)
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
  }

  @throws(classOf[Exception])
  def setJasConfigStream(dataStreamReader: DataStreamReader, sourceProperties: SourceProperties): DataStreamReader = {
    Try {
      if ((sourceProperties.properties contains "is_ssL_enabled") && (sourceProperties properties "is_ssL_enabled" equalsIgnoreCase "true")) {
        val userName = sourceProperties.properties("user")
        val password = sourceProperties.properties("password")
        val jasConfig: String = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$userName" password="$password";"""
        dataStreamReader
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.sasl.jaas.config", jasConfig)
      }
      else dataStreamReader
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }

  @throws(classOf[Exception])
  def setJasConfigBatch(dataFrameReader: DataFrameReader, sourceProperties: SourceProperties): DataFrameReader = {
    Try {
      if ((sourceProperties.properties contains "is_ssL_enabled") && (sourceProperties properties "is_ssL_enabled" equalsIgnoreCase "true")) {
        val userName = sourceProperties.properties("user")
        val password = sourceProperties.properties("password")
        val jasConfig: String = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$userName" password="$password";"""
        dataFrameReader
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.sasl.jaas.config", jasConfig)
      }
      else dataFrameReader
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }

  }
}
