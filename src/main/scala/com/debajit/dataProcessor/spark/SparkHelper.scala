package com.debajit.dataProcessor.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Debajit
 */

object SparkHelper extends LazyLogging {

  /**
   * return spark session
   *
   * @param sessionType session type
   * @return
   */
  def getAndConfigureSparkSession(sessionType: Option[String] = None, appName: Option[String]): SparkSession = {

    val conf = new SparkConf().setAppName(appName.getOrElse("Structured Streaming poc local")).setMaster(sessionType.getOrElse("local[*]")).set("spark.sql.streaming.checkpointLocation", "checkpoint")
    val sc = new SparkContext(conf)
    sc setLogLevel "WARN"
    SparkSession builder() getOrCreate()
  }

  /**
   * return spark session
   *
   * @return
   */
  def getSparkSession: SparkSession = SparkSession.builder().getOrCreate()

}
