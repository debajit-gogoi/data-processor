package org.apache.spark.sql.execution.streaming.sources

import com.debajit.dataProcessor.utils.Utils
import com.debajit.java.dataProcessor.utils.OktaUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.sources.OKTAStreamSource.getLastCommitOKTAOffsetV2
import org.apache.spark.sql.execution.streaming.sources.offset.{OKTAOffset, OKTARange}
import org.apache.spark.sql.execution.streaming.{Offset, OffsetSeqLog, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, TimeZone}
import scala.util.{Failure, Success, Try}

/**
 * @author debajit
 */

class OKTAStreamSource(sqlContext: SQLContext,
                       parameters: Map[String, String]) extends Source
  with Logging {

  private val publishedTimeFormat: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  val format = new SimpleDateFormat(publishedTimeFormat, Locale.US)
  format.setTimeZone(TimeZone.getTimeZone("UTC"))
  private val startingOffset: Option[String] = {
    parameters.contains("starting_offset") match {
      case true => Some(parameters("starting_offset"))
      case false => None
    }
  }

  def emptyDataframe(sqlContext: SQLContext): DataFrame = {
    sqlContext.internalCreateDataFrame(
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
      schema,
      isStreaming = true
    )
  }

  override def schema: StructType = {
    val schemaLocation = s"${parameters("schema_location")}/${parameters("schema")}.avsc"
    Utils getStructTypeSchemaFromAvro schemaLocation
  }

  def readOffset(end: Offset): Try[OKTAOffset] = {
    Try(end.asInstanceOf[OKTAOffset])
  }

  override def getOffset: Option[Offset] = {
    val offset: Option[OKTAOffset] = getLastCommitOKTAOffsetV2(sqlContext.sparkSession, parameters("checkpoint_dir"))
    offset match {
      case None => startingOffset match {
        case None =>
          val endDate = new Date()
          val until = format.format(endDate)
          val calendar = Calendar.getInstance()
          calendar.setTime(endDate);
          calendar.add(Calendar.MINUTE, -1)
          val from = format.format(calendar.getTime)
          Some(OKTAOffset(OKTARange(Some(from), Some(until))))
        case _ =>
          val from = startingOffset.get
          val until = getEndDateOffset(from)
          Some(OKTAOffset(OKTARange(Some(from), Some(until))))
      }
      case _ =>
        val end = getEndDateOffset(offset.get.range.end.get)
        Some(OKTAOffset(OKTARange(offset.get.range.end, Some(end))))
    }
  }

  def getEndDateOffset(startDateString: String): String = {
    val minStartDate = format.parse(startDateString)
    val currentDate = new Date()
    val diffInMillis = Math.abs(currentDate.getTime - minStartDate.getTime)
    val differenceInMinutes = diffInMillis / (1000 * 60)
    val maxOffsetsPerTrigger = parameters("max_minutes_per_trigger").toInt
    if (differenceInMinutes > maxOffsetsPerTrigger) {
      import java.util.Calendar
      val cal = Calendar.getInstance
      cal.setTime(minStartDate)
      cal.add(Calendar.MINUTE, maxOffsetsPerTrigger)
      format.format(cal.getTime)
    }
    else format.format(currentDate)
  }

  @throws(classOf[Exception])
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val oktaOffset = readOffset(end) match {
      case Success(value) => value
      case Failure(f) => logWarning(s"${f.getMessage} \nFalling back to casting using JsonMapper native methods as job has been restarted!!")
        OKTAOffset.fromJson(end.toString)
    }
    val startOffset = oktaOffset.range.start
    val endOffset = oktaOffset.range.end
    val tmpPath = parameters getOrElse("tmp-storage", "output/okta")
    OktaUtils getLogs(
      parameters("url"),
      parameters("token"),
      format parse startOffset.get,
      format parse endOffset.get,
      Integer parseInt parameters("batch_size"),
      sqlContext.sparkSession,
      schema,
      tmpPath)
    var df = emptyDataframe(sqlContext)
    df = sqlContext.sparkSession.read.schema(schema).parquet(tmpPath)
    sqlContext.internalCreateDataFrame(df.queryExecution.toRdd, schema, isStreaming = true)
  }

  override def stop(): Unit = logWarning("Stop is not implemented!")
}

object OKTAStreamSource {
  val STARTING_OFFSETS_OPTION_KEY = "startingoffset"

  def getLastCommitOffset(spark: SparkSession, checkpointRoot: String): Option[OKTAOffset] =
    Try {
      val checkpointDir = new Path(new Path(checkpointRoot), "offsets").toUri.toString
      val offsetSeqLog = new OffsetSeqLog(spark, checkpointDir)
      OKTAOffset.fromJson(offsetSeqLog.getLatest().map(x => x._2).get.offsets.head.get.toString)
    } match {
      case Success(v) => Some(v)
      case Failure(_) => None
    }

  def getLastCommitOKTAOffsetV2(spark: SparkSession, checkpointRoot: String): Option[OKTAOffset] = {
    println(s"checking checkpoint directory $checkpointRoot for offsets")
    Try {
      val checkpointDirCommits = new Path(new Path(checkpointRoot), "commits").toUri.toString
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val commitStatus = fs.listStatus(new Path(checkpointDirCommits))
      val commit = commitStatus.map(x => x.getPath.toString.split(File.separator).last.toInt).max
      val checkpointDirOffsets = new Path(new Path(checkpointRoot), s"offsets${File.separator}$commit").toUri.toString
      val out = spark.read.text(checkpointDirOffsets).takeAsList(3).get(2).getString(0)
      OKTAOffset fromJson out
    } match {
      case Success(v) => Some(v)
      case Failure(_) => None
    }
  }
}
