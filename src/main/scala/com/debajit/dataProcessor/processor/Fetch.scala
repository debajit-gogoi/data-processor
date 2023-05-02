package com.debajit.dataProcessor.processor

/**
 * @author Debajit
 */

trait Fetch {
  @throws(classOf[Exception])
  def process(input_properties: List[Map[String, String]],
              out_properties: List[Map[String, String]],
              pipeline: String,
              query: String,
              custom_process_file: String,
              custom_process_code_snippet: String,
              args: Array[String]): Unit
}

object Fetch {
  val BATCH = "batch"
  val REALTIME = "real"
}

