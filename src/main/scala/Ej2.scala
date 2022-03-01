import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Ej2 extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  val esq = StructType(Array(
    StructField("Customer_id", IntegerType, true),
    StructField("Customer_fname", StringType ,true),
    StructField("Customer_lname", StringType ,true),
    StructField("Customer_email", StringType ,true),
    StructField("Customer_password", StringType ,true),
    StructField("Customer_street", StringType ,true),
    StructField("Customer_city", StringType ,true),
    StructField("Customer_state", StringType ,true),
    StructField("Customer_zipcode", StringType ,true)
  ))



  val df = sparkSession.read.option("header", "true").schema(esq).option("sep", "\t").csv("src/main/resources/retail_db/customers-tab-delimited")

  val fil = df.filter("Customer_fname like 'A%'").groupBy("Customer_state").count().filter("count >50")


  fil.write.format("parquet").mode("overwrite").option("compression","gzip")
    .save("src/main/resources/dataset/q2/solution")


}
