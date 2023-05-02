package com.debajit.dataProcessor.sink

import com.debajit.java.dataProcessor.utils.ZoomGetParticipantsUtils
import com.typesafe.scalalogging.LazyLogging
import io.jsonwebtoken.{Jwts, SignatureAlgorithm}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util
import java.util.Date
import javax.crypto.spec.SecretKeySpec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * @author debajit
 */
object ZoomParticipantSink extends Sink with LazyLogging {
  val uri = "https://api.zoom.us/v2"
  val typeFetch = "past"
  val pageSize = 300
  val delimiter = "~~"

  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val spark = df.sparkSession
    import spark.implicits._

    val fromJob = sinkProperties properties "from_job"

    val listAcc: CollectionAccumulator[List[String]] = spark.sparkContext collectionAccumulator[List[String]] s"participant_device_$fromJob"
    val countAcc: CollectionAccumulator[String] = spark.sparkContext collectionAccumulator[String] s"participant_count_$fromJob"
    val failedAcc: CollectionAccumulator[String] = spark.sparkContext collectionAccumulator[String] s"failed_$fromJob"
    val currentTimestamp = System.currentTimeMillis()

    Try {
      df foreach (row => {
        val meetingId = row getString 0
        val startTime = row getString 1
        val attempt = row getInt 2
        val out: util.List[String] = ZoomGetParticipantsUtils getZoomParticipants(uri, meetingId, typeFetch, pageSize, getJWTToken(sinkProperties))
        if (out.size() > 0) {
          listAcc add out.asScala.toList
          countAcc add (Seq(meetingId, startTime, out.size()) mkString delimiter)
        }
        else {
          val attemptCount = attempt + 1
          if(attempt <= 3){
            failedAcc add (Seq(meetingId, startTime, currentTimestamp, attemptCount) mkString delimiter)
          }
        }
      })
      var listBuffer = ListBuffer[String]()
      val participantsList: util.List[List[String]] = listAcc.value
      val listMeetingParticipantsCount: util.List[String] = countAcc.value
      val failedFetchList: util.List[String] = failedAcc.value.asScala.distinct.asJava
      participantsList.asScala foreach (elem => {
        elem foreach (item => listBuffer += item)
      })
      val participantsDF = listBuffer.toList toDF "value" withColumn("key", lit("device"))
      listMeetingParticipantsCount.asScala.toDF("value").createOrReplaceTempView("countData")
      val participantCountByMeetingDF: DataFrame = spark.sql(s"select value, CONCAT(SPLIT(value,'$delimiter')[0],'_count') as key from countData")
      val (failedDF, failSinkProperties) = getFailedDFAndSinkProp(failedFetchList, sinkProperties, spark)
      (sinkProperties.properties contains "test") && (sinkProperties.properties("test") equalsIgnoreCase "true") match {
        case true =>
          ConsoleSink write(participantsDF, sinkProperties)
          ConsoleSink write(participantCountByMeetingDF, sinkProperties)
          ConsoleSink write(failedDF, failSinkProperties)
        case false =>
          KafkaSink write(participantsDF, sinkProperties)
          KafkaSink write(participantCountByMeetingDF, sinkProperties)
          KafkaSink write(failedDF, failSinkProperties)
      }
    }
    match {
      case Success(_) =>
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }

    /**
     * resetting the accumulators for garbage collection
     */
    listAcc reset()
    countAcc reset()
    failedAcc reset()
  }

  /**
   * get tuple of failed df and its relevant sink properties
   *
   * @param failedFetchList failed meeting details from acc
   * @param sinkProperties  original sinkproperties
   * @param spark           sparksession
   * @return
   */
  def getFailedDFAndSinkProp(failedFetchList: util.List[String], sinkProperties: SinkProperties, spark: SparkSession): (DataFrame, SinkProperties) = {
    import spark.implicits._
    val failTopic = sinkProperties.properties("topic").concat("-fail")
    val failProperties: Map[String, String] = sinkProperties.properties - "topic" + ("topic" -> failTopic)
    val failSinkProperties = SinkProperties(failProperties)
    failedFetchList.asScala.toDF("value").createOrReplaceTempView("failedData")
    val failedDF: DataFrame = spark.sql(s"select value, CONCAT(SPLIT(value,'$delimiter')[0],'_fail') as key from failedData")
    (failedDF, failSinkProperties)
  }

  /**
   * get the JWT token
   *
   * @param sinkProperties properties for sink
   * @return
   */
  @throws(classOf[Exception])
  def getJWTToken(sinkProperties: SinkProperties): String = {
    val secret = sinkProperties properties "api_secret"
    val hmacKey = new SecretKeySpec(secret.getBytes, SignatureAlgorithm.HS256.getJcaName)
    val now = Instant.now
    Try{
      Jwts.builder
        .setIssuer(sinkProperties properties "api_key")
        .setIssuedAt(Date.from(now))
        .setExpiration(Date.from(now.plus(5l, ChronoUnit.MINUTES)))
        .signWith(hmacKey)
        .compact
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }
  }
}
