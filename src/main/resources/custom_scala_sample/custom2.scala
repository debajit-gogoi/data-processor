import com.debajit.dataProcessor.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

new CustomProcess {
  override def process(df: DataFrame): DataFrame = {
    val _df = df.withColumn("test", lit(1))
    _df.show()
    _df
  }
}


