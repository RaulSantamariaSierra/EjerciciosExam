
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, lit, month, to_date, year}

object Ej1 extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val leer = sparkSession.read.parquet("src/main/resources/retail_db/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet")


/*
  val fil = leer.select(col("order_id"),
    to_date(from_unixtime(col("order_date") / 1000,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss").as("order_date"),
    col("order_status"))
    .filter("order_status = 'COMPLETE'")
    .select(col("order_id"),
      date_format(col("order_date"),"dd-MM-yyyy").as("order_date"))
    .show()

*/


  val order = leer.filter(col("order_status").equalTo("COMPLETE"))
    .select(col("order_id"), to_date(from_unixtime(col("order_date")/1000, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")
      .as("order_date"),
      col("order_status"))

  val con = order.filter(year(col("order_date")).equalTo(2014) and
    (month(col("order_date")).equalTo(1) or month(col("order_date")).equalTo(7)))
    .select(col("order_id"), date_format(col("order_date"), "dd-MM-yyyy")
      .as("order_date"), col("order_status"))


  con.write.format("json").mode("Overwrite")
    .save("src/main/resources/dataset/q1/solution")

}
