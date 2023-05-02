package org.apache.spark.sql.execution.streaming.sources

import com.debajit.dataProcessor.utils.SalesforceFetchUtils.{fetchSFUtil, getJDBCUrl, querySalesforceJDBC}
import com.debajit.java.dataProcessor.utils.SalesforceSoqlUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.offset._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import scala.util.{Failure, Success, Try}

/**
 * @author debajit
 */

class SFDCStreamSource(
                        sqlContext: SQLContext,
                        parameters: Map[String, String],
                        df: DataFrame
                      ) extends Source
  with Logging {

  import SFDCStreamSource._
  import sqlContext.implicits._

  private val typeOfExtraction =  parameters("type_of_extraction")

  private val lastModifiedDataTimeFormat: String = typeOfExtraction match {
    case "cdata" => "yyyy-MM-dd HH:mm:ss"
    case "soql" => "yyyy-MM-dd'T'HH:mm:ss'Z'"
  }

  val format = new SimpleDateFormat(lastModifiedDataTimeFormat)
  format.setTimeZone(TimeZone.getTimeZone("UTC"))
  private val offsetColumn: String =
    parameters.getOrElse(OFFSET_COLUMN, throw new IllegalArgumentException(s"Parameter not found: $OFFSET_COLUMN"))
  private val startingOffset: String = {
    val startingOffset = parameters.get(STARTING_OFFSETS_OPTION_KEY)
    val offset = startingOffset.getOrElse(EarliestOffsetRangeLimit.toString)
    offset match {
      case EarliestOffsetRangeLimit.toString => EarliestOffsetRangeLimit.toString
      case LatestOffsetRangeLimit.toString => LatestOffsetRangeLimit.toString
      case v => v
    }
  }

  @throws(classOf[Exception])
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val sfdcOffset = readOffset(end) match {
      case Success(value) => value
      case Failure(f) => println(s"${f.getMessage} \nFalling back to casting using JsonMapper native methods as job has been restarted!!")
        SFDCOffset fromJson end.toString
    }
    val startOffset: Option[String] = sfdcOffset.range.start
    val endOffset: Option[String] = sfdcOffset.range.end
    /*Try{
      Utils.deletePath( s"tmp/${parameters("tmp_storage")}/${SFDCStreamSource.genericUUID}" ,sqlContext.sparkSession)
    }
    match {
      case Success(_) => println("deleted tmp folder")
      case Failure(exception) => exception.printStackTrace()

    }*/
    val dataFrame = getDf(sqlContext, parameters, startOffset, endOffset, None)
    sqlContext.internalCreateDataFrame(dataFrame.queryExecution.toRdd, schema, isStreaming = true)
  }

  override def schema: StructType = df.schema

  def readOffset(end: Offset): Try[SFDCOffset] = {
    Try(end.asInstanceOf[SFDCOffset])
  }

  override def stop(): Unit =
    logWarning("Stop is not implemented!")

  override def getOffset: Option[Offset] = {
    getType(offsetColumn, schema)
    val offset: Option[SFDCOffset] = getLastCommitSFDCOffsetV2(sqlContext.sparkSession, parameters("checkpoint_dir"))
    println(s"initial offset $offset")
    offset match {
      case None => startingOffset match {
        case EarliestOffsetRangeLimit.toString =>
          val end = getOffsetToProcess(None).get
          Some(SFDCOffset(offsetColumn, OffsetRange(None, Some(end))))
        case LatestOffsetRangeLimit.toString =>
          val currentDate = new Date()
          val cal = Calendar.getInstance
          cal.setTime(currentDate)
          cal.add(Calendar.SECOND, -5)
          val start = format format cal.getTime
          val end = getOffsetToProcess(Some(start)).get
          Some(SFDCOffset(offsetColumn, OffsetRange(Some(start), Some(end))))
        case _ =>
          val start = startingOffset
          val end = getOffsetToProcess(Some(start)).get
          Some(SFDCOffset(offsetColumn, OffsetRange(Some(start), Some(end))))
      }
      case _ =>
        val end = getEndDateOffset(offset.get.range.end.get)
        Some(SFDCOffset(offsetColumn, OffsetRange(offset.get.range.end, Some(end))))
    }
  }

  private def getType(columnName: String, schema: StructType): DataType = {
    val sqlField = schema.fields find (_.name.toLowerCase == columnName.toLowerCase) getOrElse (throw new IllegalArgumentException(s"Column not found in schema: '$columnName'"))
    sqlField.dataType
  }

  private def getOffsetToProcess(start: Option[String]) = Try {
    start match {
      case None =>
        val minStartString = typeOfExtraction match {
          case "soql" => SalesforceSoqlUtils.querySalesforceData(parameters("url"),
            "/services/Soap/u/52.0",
            parameters("user"),
            s"${parameters("password")}${parameters("token")}",
            s"MIN($offsetColumn) $offsetColumn",
            parameters("table"),
            null,
            null,
            1,
            sqlContext.sparkSession,
            parameters("tmp_storage"),
            true,
            true)
            .select(offsetColumn).as[String].first()
          case "cdata" => val query = s"select MIN($offsetColumn) as $offsetColumn from `${parameters("table")}`"
            querySalesforceJDBC(
              spark = sqlContext.sparkSession,
              tableName = parameters("table"),
              columns = None,
              query = Some(query),
              jdbcURL = getJDBCUrl(parameters("user"), parameters("password"), parameters("token"), parameters("url")),
              startTime = None,
              endTime = None,
              condition = None,
              forSchema = false)
              .select(offsetColumn).as[String].first()
        }

        println(s"minimum retrieval date $minStartString")
        getEndDateOffset(minStartString)
      case _ =>
        getEndDateOffset(start.get)
    }
  }
  match {
    case Success(value) => Some(value)
    case Failure(ex) => logWarning(s"Not found offset ${ex.getStackTrace mkString "\n"}"); None
  }

  def getEndDateOffset(startDateString: String): String = {
    println(s"Start date -> $startDateString")
    val minStartDate = format parse startDateString
    val currentDate = new Date()
    println(s"Current date ${format format currentDate}")
    val diffInMillis = Math abs currentDate.getTime - minStartDate.getTime
    val differenceInDays = diffInMillis / (1000 * 60 * 60 * 24);
    println(s"Difference in days $differenceInDays")
    val maxOffsetsPerTrigger = parameters("max_offset_days_per_trigger").toInt
    if (differenceInDays > maxOffsetsPerTrigger) {
      val cal = Calendar.getInstance
      cal.setTime(minStartDate)
      cal.add(Calendar.DATE, maxOffsetsPerTrigger)
      format format cal.getTime
    }
    else format format currentDate
  }

}

object SFDCStreamSource {
  val STARTING_OFFSETS_OPTION_KEY = "starting_offset"
  val OFFSET_COLUMN = "offset_column"

  def getLastCommitSFDCOffset(spark: SparkSession, checkpointRoot: String): Option[SFDCOffset] = {
    println(s"checking checkpoint directory $checkpointRoot for offsets")
    Try {
      val checkpointDir = new Path(new Path(checkpointRoot), "offsets").toUri.toString
      println(s"checkpoint dir $checkpointDir")
      val offsetSeqLog = new OffsetSeqLog(spark, checkpointDir)
      SFDCOffset fromJson (offsetSeqLog getLatest() map (x => x._2)).get.offsets.head.get.toString
    } match {
      case Success(v) => Some(v)
      case Failure(_) => None
    }
  }

  def getLastCommitSFDCOffsetV2(spark: SparkSession, checkpointRoot: String): Option[SFDCOffset] = {
    println(s"checking checkpoint directory $checkpointRoot for offsets")
    Try {
      val checkpointDirCommits = new Path(new Path(checkpointRoot), "commits").toUri.toString
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val commitStatus = fs.listStatus(new Path(checkpointDirCommits))
      val commit = commitStatus.map(x => x.getPath.toString.split(File.separator).last.toInt).max
      val checkpointDirOffsets = new Path(new Path(checkpointRoot), s"offsets${File.separator}$commit").toUri.toString
      val out = spark.read.text(checkpointDirOffsets).takeAsList(3).get(2).getString(0)
      SFDCOffset fromJson out
    } match {
      case Success(v) => Some(v)
      case Failure(_) => None
    }
  }

  @throws(classOf[Exception])
  def getDf(sqlContext: SQLContext, parameters: Map[String, String], startTime: Option[String], endTime: Option[String], query: Option[String]): DataFrame = {

    parameters.contains("support_parallel_fetch") && parameters("support_parallel_fetch").equalsIgnoreCase("true") match {
      case true => fetchSFUtil(spark = sqlContext.sparkSession, parameters,
        startTime = startTime,
        endTime = endTime)
      case false =>
        Try {
          parameters("type_of_extraction") match {
            case "cdata" => querySalesforceJDBC(spark = sqlContext.sparkSession, parameters("table"), parameters.get("columns"), None,
              getJDBCUrl(parameters("user"), parameters("password"), parameters("token"), parameters("url")),
              startTime, endTime, None, false)
            case "soql" => SalesforceSoqlUtils.querySalesforceData(parameters("url"), "/services/Soap/u/52.0",
              parameters("user"),
              s"${parameters("password")}${parameters("token")}",
              parameters("columns"),
              parameters("table"),
              startTime.get,
              endTime.get,
              2000,
              sqlContext.sparkSession,
              parameters("tmp_storage"),
              false,
              true
            )
          }

        }
        match {
          case Success(value) => value
          case Failure(exception) => println(exception.printStackTrace())
            throw new RuntimeException()
        }
    }
  }
}