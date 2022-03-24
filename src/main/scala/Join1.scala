import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, desc}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Join1 extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val cus = sparkSession.read.option("header", "true").csv("src/main/resources/retail_db/customers")


  cus.show()


  val esq = StructType(Array(
    StructField("order_id", IntegerType, true),
    StructField("order_date", StringType, true),
    StructField("order_customer_id", IntegerType, true),
    StructField("order_status", StringType, true)
  ))

  val ord = sparkSession.read.option("header", "true").schema(esq).csv("src/main/resources/retail_db/orders")
  ord.show()

  val join = ord.groupBy(col("order_customer_id")).count().where(col("count").gt(5))
    .join(cus, ord.col("order_customer_id") === cus.col("customer_id") , "inner" )
    .select("customer_fname","customer_lname", "count" )
    .filter(col("customer_fname").startsWith("M"))
    .orderBy(desc("count"))

  join.show()




}
