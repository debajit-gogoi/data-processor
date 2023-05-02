package com.debajit.dataProcessor.source

import com.mongodb.spark.config.ReadConfig
import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object MongoSource extends Source {

  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    val mongoURI = sourceProperties properties "uri"
    val conf = makeMongoURI(mongoURI, sourceProperties properties "db_name",
      sourceProperties properties "collection_name")
    val readConfig: ReadConfig = ReadConfig(Map("uri" -> conf))
    val df = spark.read format "com.mongodb.spark.sql.DefaultSource" option("spark.mongodb.input.sampleSize", 500000) option("pipeline", sourceProperties properties "pipeline") options readConfig.asOptions load()
    (df, None)
  }

  def makeMongoURI(uri: String, database: String, collection: String) = s"""$uri/$database.$collection"""

}
