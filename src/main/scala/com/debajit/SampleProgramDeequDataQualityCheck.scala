package com.debajit

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.debajit.dataProcessor.spark.SparkHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, md5}

object SampleProgramDeequDataQualityCheck {
  private val spark = SparkHelper getAndConfigureSparkSession(sessionType = None, appName = Some("Deequ"))

  def main(args: Array[String]): Unit = {
    val (metricsSource, md5Source) = generateChecks("/Users/debajit.gogoi/Documents/zipcodes.csv")
    val (metricsProd, md5Prod) = generateChecks("/Users/debajit.gogoi/Documents/zipcodes_2.csv")

    assert(metricsSource
      .join(
        metricsProd, metricsSource("entity") === metricsProd("entity") &&
          metricsSource("instance") === metricsProd("instance") &&
          metricsSource("name") === metricsProd("name") &&
          metricsSource("value") === metricsProd("value"),
        "leftanti"
      ).count() == 0)


    assert(md5Source.join(md5Prod, md5Source("md5") === md5Prod("md5"), "leftanti").count() == 0)
  }

  def generateChecks(path: String): (DataFrame, DataFrame) = {
    val datasource: DataFrame = spark.read.format("csv").option("header", "true")
      .options(Map("inferSchema" -> "true", "delimiter" -> ",")).load(path)
    datasource.show(truncate = false)
    //datasource.printSchema()
    val metrics = successMetricsAsDataFrame(spark, analyseDataQuality(datasource))
    metrics.show(truncate = false)
    val md5df = md5CheckSum(datasource)
    //md5df.show(truncate = false)
    (metrics, md5df)
  }

  def md5CheckSum(dataSource: DataFrame): DataFrame = {
    dataSource
      .withColumn("md5", md5(concat_ws("~", dataSource.columns.map(column => col(column)): _*)))
      .select("md5")
  }

  def analyseDataQuality(datasource: DataFrame): AnalyzerContext = {
    val analysisResult: AnalyzerContext = {
      AnalysisRunner
        // data to run the analysis on
        .onData(datasource)
        // define analyzers that compute metrics
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("RecordNumber"))
        .addAnalyzer(CountDistinct("RecordNumber"))
        .addAnalyzer(Minimum("Lat"))
        .addAnalyzer(Mean("Lat"))
        .addAnalyzer(Maximum("Lat"))
        .addAnalyzer(Entropy("Lat"))
        .run()
    }
    analysisResult
  }

}
