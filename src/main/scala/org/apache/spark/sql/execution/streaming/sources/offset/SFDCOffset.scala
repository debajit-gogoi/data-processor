package org.apache.spark.sql.execution.streaming.sources.offset

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.execution.streaming.Offset

/**
 * @author debajit
 */

case class OffsetRange(start: Option[String], end: Option[String] ) {

  override def toString: String =
    s"start = '$start', end = '$end'"
}

case class SFDCOffset(columnName: String, range: OffsetRange) extends Offset {
  override def toString: String =
    s"Offset column = '$columnName', Offset range = '$range'"

  override def json(): String =
    SFDCOffset.toJson(SFDCOffset(columnName, range))
}

case object SFDCOffset {
  private val jsonMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    mapper
  }

  def toJson(value: SFDCOffset): String =
    jsonMapper.writeValueAsString(value)

  private def fromJson[T](json: String)(implicit m: Manifest[T]): T =
    jsonMapper.readValue[T](json)

  def fromJson(json: String): SFDCOffset = fromJson[SFDCOffset](json)
}
