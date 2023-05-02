package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import com.debajit.java.dataProcessor.utils.O365DataUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset}

import java.util
import scala.util.{Failure, Success, Try}

/**
 * @author debajit
 */

object O365Source extends Source with Logging {
  private val spark = SparkHelper.getSparkSession

  import spark.implicits._

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val uri = Utils resolveConfigVariables(sourceProperties properties "uri", sourceProperties.properties)
    logInfo(s"Calling okta endpoint : $uri")
    val tokenUri = sourceProperties.properties("token_uri")
    val grantType = sourceProperties.properties("grant_type")
    val clientId = sourceProperties.properties("client_id")
    val clientSecret = sourceProperties.properties("client_secret")
    val scope = sourceProperties.properties("scope")
    val data: Try[util.List[String]] = Try(O365DataUtils.o365Data(uri, tokenUri, grantType, clientId, clientSecret, scope))
    val df: DataFrame = data match {
      case Success(value) =>
        val csvData: Dataset[String] = spark.createDataset(value)
        spark.read.option("header", "true").option("inferSchema", "true").csv(csvData)
      case Failure(exception) =>
        logError(s"${exception.getMessage} \n Issue in Retrieving data")
        throw new RuntimeException()
    }

    (Utils removeSpecialCharacterFromDFColName(df, ""), None)
  }
}
