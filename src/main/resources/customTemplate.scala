import com.debajit.dataProcessor.utils._
import org.apache.spark.sql.DataFrame

new CustomProcess {
  override def process(df: DataFrame): DataFrame = {
    customCodeSnippetImplementation(df)
  }

  def customCodeSnippetImplementation(df: DataFrame): DataFrame = {
    %%%% _custom_code_here %%%%
  }
}


