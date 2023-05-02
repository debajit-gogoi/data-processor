package com.debajit.dataProcessor.processor.realtimeFetch

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper.getSparkSession
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Debajit
 */

class BasicReal extends Fetch with Logging {
  val spark: SparkSession = getSparkSession

  @throws(classOf[Exception])
  override def process(input_properties: List[Map[String, String]], out_properties: List[Map[String, String]], pipeline: String, query: String, custom_process_file: String, custom_process_code_snippet: String, args: Array[String]): Unit = {
    val query_variables: Map[String, String] = args.length > 0 match {
      case true => Utils getQueryVariablesFromArgs(args, 0)
      case false => Map.empty[String, String]
    }
    input_properties.par.foreach(properties => {
      val input: Option[(DataFrame, Option[Any])] =
        Utils callRead(properties ++ query_variables, pipeline)
      input.get._1.createOrReplaceTempView(s"${properties("alias")}")
    })
    val computed_input = spark sql query
    computed_input
      .writeStream
      .option("checkpointLocation", input_properties.head("checkpoint_dir"))
      .trigger(Trigger.ProcessingTime(input_properties.head("window")))
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
          out_properties.par.foreach(properties => {
            batchDF createOrReplaceTempView pipeline
            val input_map = Map("process" -> pipeline)
            val resolved_query = Utils resolveConfigVariables(properties("query"), input_map)
            logInfo(s"executing query for pipeline: $pipeline : $resolved_query")
            val out: DataFrame = batchDF.sparkSession sql resolved_query
            (Utils getMethodThroughScalaReflection(
              class_name = properties("action_class"),
              method_name = "write")) (out, Some(batchId), properties)
          })
      } start()
  }
}
