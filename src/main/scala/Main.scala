import org.apache.spark.sql.SparkSession

object Main extends App {

  val valueForString = "data"
  print(valueForString)

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val df = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/retail_db/orders_parquet")

    df.show()




}
