package org.apache.spark.sql.execution.streaming.sources

import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * @author debajit
 */
class OKTAStreamSourceProvider extends StreamSourceProvider with DataSourceRegister {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
    val schemaLocation = s"${parameters("schema_location")}/${parameters("schema")}.avsc"
    (shortName(), Utils getStructTypeSchemaFromAvro schemaLocation)
  }

  override def shortName(): String = "okta-streaming"

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    val caseInsensitiveParameters: CaseInsensitiveMap[String] = CaseInsensitiveMap(parameters)
    new OKTAStreamSource(sqlContext, caseInsensitiveParameters)
  }
}
