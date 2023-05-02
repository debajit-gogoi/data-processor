package com.debajit.dataProcessor.app

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.structs.LogMessage
import com.debajit.dataProcessor.utils.Utils

import java.io.{PrintWriter, StringWriter}
import java.net.InetAddress

/**
 * @author Debajit
 */

object BatchReadWriteApp {
  def main(args: Array[String]): Unit = {
    val batch_config = System.getProperty("batch_config")
    val batch_enabled = System.getProperty("batch_enabled")
    val appName: Option[String] = Utils getAppRunEnv(batch_config, "appName")
    val sessionType = Utils getAppRunEnv(batch_config, "sessionType")
    val enabled_pipeline_configs: Map[String, Map[String, String]] = Utils getEnabledPipelines batch_enabled
    val spark = SparkHelper getAndConfigureSparkSession(sessionType = sessionType, appName = appName)

    try {
      enabled_pipeline_configs foreach { case (pipeline, pipeline_properties) =>
        val runningInstance = uuid
        println(s"running $pipeline with instance id $runningInstance at ${System.currentTimeMillis()}")

        val (class_name: String, input_properties: List[Map[String, String]],
        output_properties: List[Map[String, String]], query: String, custom_process_file: String, custom_process_code_snippet: String)
        = Utils getPipelineProperties(batch_config, pipeline)

        val final_input_properties: Seq[Map[String, String]] = input_properties map
          (properties => properties ++ pipeline_properties ++ Map("process" -> Fetch.BATCH))
        (Utils getMethodThroughScalaReflection(
          class_name = class_name,
          method_name = "process")) (
          final_input_properties,
          output_properties,
          pipeline,
          query,
          custom_process_file,
          custom_process_code_snippet,
          args
        )
        println(s"completed running $pipeline with instance id $runningInstance at ${System.currentTimeMillis()}")
      }
    }
    catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        val logMessage = LogMessage(
          appName = spark.sparkContext.appName,
          appId = spark.sparkContext.applicationId,
          errorMessage = e.toString,
          stackTrace = sw.toString,
          hostName = InetAddress.getLocalHost.getHostName
        )

        val msg = Utils.getErrorSummary(logMessage) +
          s"\n StackTrace :- \n " +
          s"${logMessage.stackTrace}"
        println(msg)
        System.exit(1)
    }
    spark stop()
  }

  def uuid: String = java.util.UUID.randomUUID.toString
}
