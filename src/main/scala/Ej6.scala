
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Ej6 {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val esq = StructType(Array(
    StructField("Category_id", IntegerType, true),
    StructField("Category_department_id", IntegerType ,true),
    StructField("Category_name", StringType ,true)
  ))

  val df = sparkSession.read.option("header", "true").schema(esq).csv("src/main/resources/retail_db/categories/part-m-00000")

  val con = df.filter("Category_name = 'Soccer'")

  con.write.format("csv").mode("overwrite").option("sep"," ")
    .save("src/main/resources/dataset/q6/solution")



}
