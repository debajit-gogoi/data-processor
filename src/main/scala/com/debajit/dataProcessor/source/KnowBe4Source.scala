package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.java.dataProcessor.utils.KnowBe4DataUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.json.JSONObject

import java.io.File
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

/**
 * @author debajit
 */

object KnowBe4Source extends Source with Logging {

  private val spark = SparkHelper.getSparkSession

  import spark.implicits._

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val uri = sourceProperties.properties("uri")
    logInfo(s"Calling KnowBe4 Endpoint : $uri")
    val token = sourceProperties.properties("token")
    val pageSize = sourceProperties.properties("page_size").toInt
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tmpPath = sourceProperties.properties.getOrElse("tmp_storage", "output/knowbe4")
    val pathToPersist = new Path(tmpPath)
    if (fs.exists(pathToPersist) && fs.isDirectory(pathToPersist))
      fs.delete(pathToPersist, true)
    var page = 1
    breakable {
      while (true) {
        val data: Try[java.util.List[JSONObject]] = Try(KnowBe4DataUtils.getKnowBe4Data(uri, token, pageSize, page))
        data match {
          case Success(value) =>
            if (value.size() == 0) break
            val df = spark.read.json(Seq(value.toString).toDS)
            println(s"Writing to $tmpPath${File.separator}${sourceProperties properties "alias"} for $uri and page $page")
            df.write.mode(SaveMode.Append).json(s"$tmpPath${File.separator}${sourceProperties properties "alias"}")
            page = page + 1;
          case Failure(exception) =>
            logError(s"${exception.getMessage} \n Issue in Retrieving data")
            throw new RuntimeException()
        }
      }
    }
    val df = spark.read.json(s"$tmpPath${File.separator}${sourceProperties properties "alias"}")
    (df, None)
  }
}
