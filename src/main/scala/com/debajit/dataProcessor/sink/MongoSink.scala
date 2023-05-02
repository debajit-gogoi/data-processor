package com.debajit.dataProcessor.sink

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.debajit.dataProcessor.source.MongoSource.makeMongoURI
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object MongoSink extends Sink {
  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val mongoURI = sinkProperties properties "uri"
    val conf = makeMongoURI(mongoURI, sinkProperties properties "db_name",
      sinkProperties properties "collection_name")
    MongoSpark.save(df.write, WriteConfig(Map("uri" -> conf)))
  }
}
