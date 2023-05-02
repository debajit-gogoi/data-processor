package com.debajit.dataProcessor.utils

import org.apache.spark.sql.DataFrame

import scala.io.Source
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

/**
 * @author Debajit
 */


abstract class CustomProcess {
  def process(df: DataFrame): DataFrame
}

case class CustomSparkCode(customProcessFile: String, CustomProcessSnippet: String) {

  val CUSTOM_LOGIC_PLACEHOLDER = "%%%%_custom_code_here%%%%"

  val codeToExecute: String = CustomProcessSnippet.isEmpty() match {
    case false => Utils createExecutionCodeFromSnippet(CustomProcessSnippet, CUSTOM_LOGIC_PLACEHOLDER)
    case true => customProcessFile isEmpty match {
      case false => Source.fromFile(customProcessFile).getLines.mkString("\n")
      case true => Utils createExecutionCodeFromSnippet("df", CUSTOM_LOGIC_PLACEHOLDER)
    }

  }
  val toolbox = currentMirror.mkToolBox()
  println(codeToExecute)
  val tree = toolbox.parse(codeToExecute)
  val compiledCode = toolbox.compile(tree)

  def make(): CustomProcess = compiledCode().asInstanceOf[CustomProcess]
}