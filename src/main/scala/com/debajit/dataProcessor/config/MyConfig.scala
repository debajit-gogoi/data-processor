package com.debajit.dataProcessor.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

/**
 * @author Debajit
 */

class MyConfig private(fileNameOption: Option[String] = None) {
  val config: Config = fileNameOption.fold(
    ifEmpty = ConfigFactory.load())(
    file => ConfigFactory.load(file))

  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}

object MyConfig {
  val dateTimeFormat = "yyyyMMdd HH:mm"
  val dateFormat = "yyyyMMdd"
  val timeFormat = "HH:mm"
  val dateWithHourFormat = "yyyyMMdd HH"
  val timeHourFormat = "HH"
  val dayOfWeekFormat = "u"

  def apply(fileNameOption: Option[String] = None): MyConfig = new MyConfig(fileNameOption)
}
