
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

object Ej3 extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val cus = sparkSession.read.format("avro").load("src/main/resources/retail_db/customers-avro/*")

  val con = cus.select(col("customer_id"),concat_ws(" ", col("customer_fname"), col("customer_lname")).as("customer_name"))

    con.write.format("csv").mode("overwrite").option("sep","\t").option("compression","bzip2")
    .save("src/main/resources/dataset/q3/solution")

}
