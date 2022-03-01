import org.apache.spark.sql._


object Ej4 extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val prod = sparkSession.read.format("avro").load("src/main/resources/retail_db/products_avro/*")

  val out = prod.filter("product_name like 'Nike%' and product_price <=23 and product_price >=20")

  out.write.format("parquet").mode("overwrite").option("compression","gzip")
    .save("src/main/resources/dataset/q4/solution")
}
