package com.debajit

import com.debajit.dataProcessor.utils.SalesforceReceiver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SampleProgramSalesforceCDC extends Logging {

  def main(args: Array[String]): Unit = {

    /*if (args.length < 4 || args.length > 5) {
      System.err.println("Usage: [url] [username] [password] [topic]")
      System.exit(1)
    }*/

    // Configure logging
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example. To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val stringsStream = ssc.receiverStream(new SalesforceReceiver(this.getClass.getSimpleName,
      "https://login.salesforce.com", "", "", "/data/CaseChangeEvent"))

    stringsStream.print()


    // Start application
    ssc.start()
    ssc.awaitTermination()

  }
}
