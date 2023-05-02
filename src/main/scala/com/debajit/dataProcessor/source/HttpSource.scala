package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.DataFrame
import scalaj.http._

import scala.util.parsing.json.JSON

/**
 * @author Debajit
 */

object HttpSource extends Source {
  private val spark = SparkHelper.getSparkSession

  import spark.implicits._

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val restProperties: RestProperties = getRestProperties(sourceProperties)
    val httpRequest = getHttpRequest(restProperties)
    addAdditionalParams(httpRequest, restProperties.connStr, restProperties.oauthCredStr, restProperties.userCredStr)
    val resp = restProperties.respType match {
      case "BODY" => httpRequest.asString.body
      case "BODY-BYTES" => httpRequest.asBytes.body
      case "CODE" => httpRequest.asString.code
      case "HEADERS" => httpRequest.asString.headers
      case "LOCATION" => httpRequest.asString.location mkString " "
    }
    var df = spark.emptyDataFrame
    df = spark.read.json(Seq(resp.toString).toDS)
    (df, None)
  }

  def addHeaders(httpObj: HttpRequest, cHeaders: String): HttpRequest = {
    val headersParsed = (JSON parseFull cHeaders).get.asInstanceOf[Map[String, String]]
    httpObj.headers(headersParsed)
  }

  def addAdditionalParams(httpRequest: HttpRequest, connStr: String, oauthCredStr: String, userCredStr: String): Unit = {
    val connProp = connStr.split(""":""").map((cons: String) => cons.toInt)
    httpRequest.timeout(connTimeoutMs = connProp(0),
      readTimeoutMs = connProp(1))
    httpRequest.option(HttpOptions.allowUnsafeSSL)

    oauthCredStr == "" match {
      case true =>
        if (userCredStr != "") {
          val usrCred = userCredStr split ":"
          httpRequest.auth(usrCred(0), usrCred(1))
        }
      case false =>
        val oauthd = oauthCredStr split ":"
        val consumer = Token(oauthd(0), oauthd(1))
        val accessToken = Token(oauthd(2), oauthd(3))
        httpRequest.oauth(consumer, accessToken)
    }
  }

  def addQryParmToUri(uri: String, data: String): String = if (uri contains "?") s"""$uri&$data""" else s"""$uri?$data"""

  def getRestProperties(sourceProperties: SourceProperties): RestProperties = {
    RestProperties(uri = sourceProperties.properties("uri"),
      data = sourceProperties.properties("data"),
      customHeaders = sourceProperties.properties("custom_headers"),
      connStr = sourceProperties.properties("timeouts"),
      respType = sourceProperties.properties("respType"),
      oauthCredStr = sourceProperties.properties("oauthCredStr"),
      userCredStr = sourceProperties.properties("userCredStr"),
      method = sourceProperties.properties("method"))
  }

  def getHttpRequest(restProperties: RestProperties): HttpRequest = {
    restProperties.method.toUpperCase match {
      case "GET" => addHeaders(Http(addQryParmToUri(restProperties.uri, restProperties.data)), restProperties.customHeaders)
      case "POST" => addHeaders(Http(restProperties.uri).postData(restProperties.data), restProperties.customHeaders)
      case _ => addHeaders(Http(addQryParmToUri(restProperties.uri, restProperties.data)), restProperties.customHeaders)
    }
  }
}

case class RestProperties(uri: String,
                          data: String,
                          customHeaders: String,
                          connStr: String,
                          respType: String,
                          oauthCredStr: String,
                          userCredStr: String,
                          method: String)
