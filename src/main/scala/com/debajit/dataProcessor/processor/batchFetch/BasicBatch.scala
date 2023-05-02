package com.debajit.dataProcessor.processor.batchFetch

import com.debajit.dataProcessor.processor.Fetch
import com.debajit.dataProcessor.spark.SparkHelper.getSparkSession
import com.debajit.dataProcessor.utils.{CustomSparkCode, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util

/**
 * @author Debajit
 */

class BasicBatch extends Fetch with Logging {
  val spark: SparkSession = getSparkSession

  @throws(classOf[Exception])
  override def process(input_properties: List[Map[String, String]], out_properties: List[Map[String, String]], pipeline: String, query: String, custom_process_file: String, custom_process_code_snippet: String, args: Array[String]): Unit = {
    val query_variables = args.length > 0 match {
      case true => Utils getQueryVariablesFromArgs(args, 0)
      case false => Map.empty[String, String]
    }
    val list_post_process = new util.ArrayList[(String, String, Option[Any])]()
    input_properties.par.foreach(properties => {
      val input: Option[(DataFrame, Option[Any])] =
        Utils callRead(properties ++ query_variables, pipeline)

      val df = properties.contains("custom_preprocess_file") || properties.contains("custom_preprocess_code_snippet") match {
        case true => CustomSparkCode(properties.getOrElse("custom_preprocess_file", ""),
          properties.getOrElse("custom_preprocess_code_snippet", "")) make() process input.get._1
        case false => input.get._1
      }
      df.createOrReplaceTempView(s"${properties("alias")}")

      if (properties("format") equals "kafka") list_post_process.add((properties("topic"), properties("commit"), input.get._2))
    })

    val processed_df = if (custom_process_file.nonEmpty || custom_process_code_snippet.nonEmpty)
      CustomSparkCode(custom_process_file, custom_process_code_snippet) make() process (spark sql query)
    else spark sql query

    processed_df createOrReplaceTempView pipeline



    /*import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
    import spark.implicits._

    val suggestionResult = { ConstraintSuggestionRunner()
      .onData(processed_df)
      .addConstraintRules(Rules.DEFAULT)
      .run()
    }
    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
    }.toSeq.toDS()

    suggestionDataFrame.toJSON.collect.foreach(println)
    suggestionDataFrame.show()*/


    val input_map = Map("process" -> pipeline)
    out_properties.par.foreach(properties => {
      val resolved_query = Utils resolveConfigVariables(properties("query"), input_map)
      logInfo(s"executing query for pipeline: $pipeline : $resolved_query")
      val out: DataFrame = spark sql resolved_query
      (Utils getMethodThroughScalaReflection(
        class_name = properties("action_class"),
        method_name = "write")) (out, None, properties)
    })
    Utils commitSync list_post_process
  }
}
