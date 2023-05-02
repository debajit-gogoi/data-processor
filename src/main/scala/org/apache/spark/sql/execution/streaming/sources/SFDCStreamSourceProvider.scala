package org.apache.spark.sql.execution.streaming.sources

import com.debajit.dataProcessor.utils.SalesforceFetchUtils._
import com.debajit.java.dataProcessor.utils.SalesforceSoqlUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType


/**
 * @author debajit
 */

class SFDCStreamSourceProvider extends StreamSourceProvider with DataSourceRegister {

  private lazy val df = (sqlContext: SQLContext, parameters: Map[String, String]) => {
    println("Retrieving the schema!!")
    val typeOfExtraction: String = parameters("type_of_extraction")
    typeOfExtraction match {
      case "soql" => SalesforceSoqlUtils querySalesforceData(parameters("url"),
        "/services/Soap/u/52.0",
        parameters("user"),
        s"${parameters("password")}${parameters("token")}",
        parameters("columns"),
        parameters("table"),
        null,
        null,
        1,
        sqlContext.sparkSession,
        parameters("tmp_storage"),
        true,
        true)
      case "cdata" => querySalesforceJDBC(spark = sqlContext.sparkSession,
        tableName = parameters("table"),
        columns = Some(parameters("columns")),
        query = None,
        jdbcURL = getJDBCUrl(parameters("user"), parameters("password"), parameters("token"), parameters("url")),
        startTime = None,
        endTime = None,
        condition = None,
        forSchema = true)
    }
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = (shortName(), df(sqlContext, parameters).schema)

  override def shortName(): String = "sfdc-streaming"

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    val caseInsensitiveParameters: CaseInsensitiveMap[String] = CaseInsensitiveMap(parameters)
    new SFDCStreamSource(sqlContext, caseInsensitiveParameters, df(sqlContext, parameters))
  }
}

