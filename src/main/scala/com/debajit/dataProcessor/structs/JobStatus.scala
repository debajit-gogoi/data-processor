package com.debajit.dataProcessor.structs

object JobStatus extends Enumeration {
  type JobStatus = Value

  // Assigning values
  val started = Value("Started")
  val running = Value("Running")
  val failed = Value("Failed")
}
