import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  udf,
  col
}

val jsonDirStr = "/home/hynso/SpiderOak Hive/Learning/Udacity/DataEngineering/3-DataLakesWithSpark/Lesson2/data"
val jsonFileStr = "sparkify_log_small.json"
val jsonPathStr = s"$jsonDirStr/$jsonFileStr"


val spark = SparkSession
  .builder
  .appName("Sandbox Application")
  .master("local[*]")
  .getOrCreate()

//spark.sparkContext.setLogLevel("ERROR")
//
//import spark.implicits._
//
//val logData = spark.read.json(jsonPathStr)
//
//logData.show()
//logData.printSchema()
//logData.select("artist").show()
//logData.select($"artist", $"song").show()
//logData.filter($"length" > 1000).show()
//logData.select("artist").distinct().orderBy("artist").show()
//logData.select("page").distinct().orderBy("page").show()
//
//val multTen = udf((x: Double) => x * 10)
//logData
//  .withColumn("lengthMultTen", multTen($"length"))
//  .select($"length", $"lengthMultTen")
//  .show()
//
//val flagDowngrade = udf((pageEvent: String) => if (pageEvent == "Submit Downgrade") 1 else 0)
//logData
//  .filter($"userId".equalTo("1138"))
//  .withColumn("submitDowngrade", flagDowngrade($"page"))
//  .select($"page", $"submitDowngrade")
//  .show(200)



spark.stop()
