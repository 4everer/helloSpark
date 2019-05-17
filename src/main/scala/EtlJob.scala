import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import com.github.mrpowers.spark.daria.sql.EtlDefinition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


class EtlJob(name: String) extends ETL {
  val schema = new StructType()
    .add("VendorID", IntegerType, nullable = true)
    .add("tpep_pickup_datetime", StringType, nullable = true)
    .add("tpep_dropoff_datetime", StringType, nullable = true)
    .add("passenger_count", IntegerType, nullable = true)
    .add("trip_distance", DoubleType, nullable = true)
    .add("RatecodeID", IntegerType, nullable = true)
    .add("store_and_fwd_flag", StringType, nullable = true)
    .add("PULocationID", IntegerType, nullable = true)
    .add("DOLocationID", IntegerType, nullable = true)
    .add("payment_type", IntegerType, nullable = true)
    .add("fare_amount", DoubleType, nullable = true)
    .add("extra", DoubleType, nullable = true)
    .add("mta_tax", DoubleType, nullable = true)
    .add("tip_amount", DoubleType, nullable = true)
    .add("tolls_amount", DoubleType, nullable = true)
    .add("improvement_surcharge", DoubleType, nullable = true)
    .add("total_amount", DoubleType, nullable = true)

  def extractDF(): DataFrame = {
    // CSV options
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load("src/main/resources/yellow_tripdata_*.csv")
    df
  }

  def transform()(df: DataFrame): DataFrame = {
    val timeFmt = "yyyy-MM-dd HH:mm:ss"
    def udate_diff_nano = udf((d1: String, d2: String) => {
      val dtFormatter = DateTimeFormatter.ofPattern(timeFmt)
      val dt1 = LocalDateTime.parse(d1, dtFormatter)
      val dt2 = LocalDateTime.parse(d2, dtFormatter)

      dt1.getLong(ChronoField.NANO_OF_DAY) - dt2.getLong(ChronoField.NANO_OF_DAY)
      }
    )

    df.withColumn("Duration", udate_diff_nano(col("tpep_dropoff_datetime"), col("tpep_pickup_datetime")))
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), timeFmt).cast(TimestampType))
      .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime"), timeFmt).cast(TimestampType))
  }
  def load()(df: DataFrame): Unit = println("not for now")


  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.name)
    .config("spark.master", "local")
    .getOrCreate()
//  val extractedDF = this.extractDF()
}

object EtlJob {
  def main(args: Array[String]) {
  //Create a SparkContext to initialize Spark
  val etlJob = new EtlJob("NYTaxi")
  val etl = EtlDefinition(
    sourceDF = etlJob.extractDF(),
    transform = etlJob.transform(),
    write = etlJob.load()
  )
  etl.process()
}
}


