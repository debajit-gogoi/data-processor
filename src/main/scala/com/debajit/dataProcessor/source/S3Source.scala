package com.debajit.dataProcessor.source

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.DataFrame

/**
 * @author Debajit
 */

object S3Source extends Source {
  private val spark = SparkHelper.getSparkSession

  @throws(classOf[Exception])
  override def read(sourceProperties: SourceProperties, process: Option[String], pipeline: Option[String]): (DataFrame, Option[Any]) = {
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", sourceProperties.properties("access_key"))
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", sourceProperties.properties("secret_key"))
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark.sparkContext
      .hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    (spark.read.option("header", "true").json(Utils resolveConfigVariables(sourceProperties.properties("bucket"), sourceProperties.properties)), None)
  }
}
