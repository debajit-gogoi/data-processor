package com.debajit.dataProcessor.app

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import com.debajit.dataProcessor.utils.Utils.ssc

import scala.language.postfixOps

/**
 * @author Debajit
 */

object RealTimeStreamReadWriteApp {
  def main(args: Array[String]): Unit = {
    val realtime_config = System.getProperty("realtime_config")
    val realtime_enabled = System.getProperty("realtime_enabled")
    val appName: Option[String] = Utils getAppRunEnv(realtime_config, "appName")
    val sessionType = Utils getAppRunEnv(realtime_config, "sessionType")
    val (pipeline, pipeline_properties): (String, Map[String, String]) = Utils getEnabledPipelines realtime_enabled head
    val spark = SparkHelper getAndConfigureSparkSession(sessionType = sessionType, appName = appName)

    Utils initSparkStreamingContext pipeline_properties
    val (class_name: String, input_properties: List[Map[String, String]],
    output_properties: List[Map[String, String]], query: String, custom_process_file: String, custom_process_code_snippet: String)
    = Utils getPipelineProperties(realtime_config, pipeline)
    (Utils getMethodThroughScalaReflection(
      class_name = class_name,
      method_name = "process")) (
      input_properties,
      output_properties,
      pipeline,
      query,
      custom_process_file,
      custom_process_code_snippet,
      args
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
