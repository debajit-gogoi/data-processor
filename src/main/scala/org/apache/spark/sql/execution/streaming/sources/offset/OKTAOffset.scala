package org.apache.spark.sql.execution.streaming.sources.offset

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.execution.streaming.Offset

/**
 * @author debajit
 */

case class OKTARange(start: Option[String], end: Option[String]) {
  override def toString: String =
    s"start = '$start', end = '$end'"
}

case class OKTAOffset(range: OKTARange) extends Offset {
  override def toString: String =
    s"Offset range = '$range'"

  override def json(): String = OKTAOffset.toJson(OKTAOffset(range))
}

case object OKTAOffset {
  private val jsonMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    mapper
  }

  def toJson(value: OKTAOffset): String =
    jsonMapper.writeValueAsString(value)

  private def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    jsonMapper.readValue[T](json)

  def fromJson(json: String): OKTAOffset = fromJson[OKTAOffset](json)
}

