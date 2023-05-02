import com.debajit.dataProcessor.utils._
import org.apache.spark.sql.DataFrame

new CustomProcess {
  override def process(df: DataFrame): DataFrame = {
    val _df = df.select("user_id").limit(10)
    _df.show()
    _df
  }
}


