import org.apache.spark.sql.{DataFrame, SparkSession}

trait ETL {
  val spark: SparkSession
  def extractDF(): DataFrame
  def transform()(df: DataFrame): DataFrame
  def load()(df: DataFrame): Any
}
