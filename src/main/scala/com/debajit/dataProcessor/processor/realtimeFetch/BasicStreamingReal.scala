package com.debajit.dataProcessor.processor.realtimeFetch

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.source.KafkaSource
import com.debajit.dataProcessor.spark.SparkHelper.getSparkSession
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

/**
 * @author Debajit
 */

class BasicStreamingReal extends Fetch with Logging {
  val spark: SparkSession = getSparkSession

  @throws(classOf[Exception])
  override def process(input_properties: List[Map[String, String]], out_properties: List[Map[String, String]], pipeline: String, query: String, custom_process_file: String, custom_process_code_snippet: String, args: Array[String]): Unit = {
    val query_variables: Map[String, String] = args.length > 0 match {
      case true => Utils getQueryVariablesFromArgs(args, 0)
      case false => Map.empty[String, String]
    }
    val properties_input = input_properties.head ++ query_variables
    val properties_static = input_properties.last ++ Map("process" -> Fetch.REALTIME)
    val stream = KafkaSource readJsonAsStringFromKafkaRealTimeGroup(
      topic = properties_input("topic"),
      groupId = properties_input("group_id"),
      bootstrap_servers = properties_input("bootstrap_servers")
    )
    val schemaLocation = s"${properties_input("schema_location")}/${properties_input("schema")}.avsc"

    stream.foreachRDD {
      rdd => {
        if (!rdd.isEmpty()) {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val topicValueStrings = rdd map (record => (record value()).toString)
          val input_static: Option[(DataFrame, Option[Any])] = Utils callRead(properties_static, pipeline)
          input_static.get._1 createOrReplaceTempView s"${properties_static("alias")}"
          spark.
            read schema (Utils getStructTypeSchemaFromAvro schemaLocation) json topicValueStrings createOrReplaceTempView s"${properties_input("alias")}"
          spark sql query createOrReplaceTempView pipeline
          out_properties.par.foreach(properties => {
            val input_map = Map("process" -> pipeline)
            val resolved_query = Utils resolveConfigVariables(properties("query"), input_map)
            logInfo(s"executing query for pipeline: $pipeline : $resolved_query")
            val out: DataFrame = spark sql resolved_query
            (Utils getMethodThroughScalaReflection(
              class_name = properties("action_class"),
              method_name = "write")) (out, None, properties)
          })
          val commit_offset: Boolean = properties_input contains "commit_offset" match {
            case false => true
            case true => properties_input("commit_offset") match {
              case "false" => false
              case _ => true
            }
          }
          if (commit_offset) try stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          catch {
            case ex: Exception =>
              logError(s"Error in committing offsets. Error: ${ex.toString}")
          }
        }
      }
    }
  }
}
