import org.apache.spark.sql.SparkSession

object SparkEmployees extends App {
  val spark = SparkSession
    .builder()
    .appName("SparkHR")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
    .option("delimiter", ",")
    .load("c:\\users\\yelad\\PyCharmProjects\\m2i\\data\\hr\\employees.csv")

  df.printSchema()

  df.show()

  df.createOrReplaceTempView("V_EMPLOYEES")

  val df2 = spark.sql("SELECT COUNT(*) FROM V_EMPLOYEES");
  df2.show()

  df.columns.foreach(println)
  df.select("LAST_NAME").show()
  df.select("SALARY").summary().show()
}