package org.apache.spark.sql.execution.streaming.sources.offset

/**
 * @author debajit
 */
sealed trait SFDCOffsetRangeLimit

case object EarliestOffsetRangeLimit extends SFDCOffsetRangeLimit {
  override val toString: String = "earliest"
}

case object LatestOffsetRangeLimit extends SFDCOffsetRangeLimit {
  override val toString: String = "latest"
}

case class SpecificOffsetRangeLimit(offset: String) extends SFDCOffsetRangeLimit {
  override def toString: String = offset
}
