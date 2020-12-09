import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark = SparkSession
    .builder()
    .appName("SparkHR")
    .config("spark.master", "local")
    .getOrCreate()

  // Pour changer le niveau de journalisation/log
  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
    .option("delimiter", ",")
    .load("c:\\users\\yelad\\PyCharmProjects\\m2i\\data\\hr\\employees.csv")

  df.printSchema()

  // Aggrégations pour chaque niveau de groupement
  df.groupBy("DEPARTMENT_ID", "JOB_ID").sum("SALARY").show()

  // Aggrégations et sous-totaux "subtotals" pour chaque niveau
  df.cube("DEPARTMENT_ID", "JOB_ID").sum("SALARY").show()

  // Aggrégations et sous-totaux pour le premier niveau
  df.rollup("DEPARTMENT_ID", "JOB_ID").sum("SALARY").show()
}