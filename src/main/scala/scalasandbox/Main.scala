package scalasandbox

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  LongType,
  DoubleType
}
import org.apache.spark.sql.functions.{
  col,
  udf,
  to_timestamp,
  hour,
  dayofmonth,
  weekofyear,
  month,
  year,
  dayofweek
}


object Main {

  def main(args: Array[String]): Unit = {

    val awsAccessKeyId = "AKIASMUDD4C2PYNRG7UW"
    val awsSecretAccessKey = "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"

    val inputData = "s3a://udacity-dend/"
    val outputData = "s3://ldnicolasmay-bucket/"

    val spark = SparkSession
      .builder
      .appName("Sandbox Application")
      .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.0")
      .config("fs.s3a.access.key", awsAccessKeyId)
      .config("fs.s3a.secret.key", awsSecretAccessKey)
      .config("fs.s3.awsAccessKeyId", awsAccessKeyId)
      .config("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    // get filepath to songData json files
    val songData = inputData + "song_data/A/*/*/*.json"
    println(songData)

    // define schema for songData
    val songDataSchema = StructType(Seq(
      StructField("artist_id", StringType, nullable = true),
    StructField("artist_latitude", DoubleType, nullable = true),
    StructField("artist_location", StringType, nullable = true),
    StructField("artist_longitude", DoubleType, nullable = true),
    StructField("artist_name", StringType, nullable = true),
    StructField("duration", DoubleType, nullable = true),
    StructField("num_songs", LongType, nullable = true),
    StructField("song_id", StringType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("year", LongType, nullable = true)
    ))

    // read songData json files
    println("\n---- dfSong ----\n")
    val dfSong = spark.read
      .schema(songDataSchema)
      .json(songData)
    dfSong.printSchema()
//    dfSong.show(10)


    // extract columns to create songsTable
    println("\n---- songsTable ----\n")
    val songsTableCols = Seq("song_id", "title", "artist_id", "artist_name", "year", "duration")
    val songsTable = dfSong
      .select(songsTableCols.map(col): _*)
      .dropDuplicates()
    songsTable.printSchema()
//    songsTable.show(10)
//    songsTable.describe().show()

    // write songsTable to parquet files
    songsTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/songs_table_parquet")


    // extract columns to create artistsTable
    println("\n---- artistsTable ----\n")
    val artistsTableCols = Seq("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    val artistsTable = dfSong
      .select(artistsTableCols.map(col): _*)
      .dropDuplicates()
    artistsTable.printSchema()
//    artistsTable.show(10)
//    artistsTable.describe().show()

    // write artistsTable to parquet files
    artistsTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/artists_table_parquet")


    // get filepath to logData json files
    val logData = inputData + "log_data/*/*/*.json"
    println(logData)

    // define schema for logData
    val logDataSchema = StructType(Seq(
      StructField("artist", StringType, nullable = true),
      StructField("auth", StringType, nullable = true),
      StructField("firstName", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("itemInSession", LongType, nullable = true),
      StructField("lastName", StringType, nullable = true),
      StructField("length", DoubleType, nullable = true),
      StructField("level", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("method", StringType, nullable = true),
      StructField("page", StringType, nullable = true),
      StructField("registration", DoubleType, nullable = true),
      StructField("sessionId", LongType, nullable = true),
      StructField("song", StringType, nullable = true),
      StructField("status", LongType, nullable = true),
      StructField("ts", LongType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("userId", StringType, nullable = true)
    ))

    // read logData json files
    println("\n---- dfLog ----\n")
    val dfLog = spark.read
      .schema(logDataSchema)
      .json(logData)
    dfLog.printSchema()
//    dfLog.show(10)

    val dfLogRenamed = dfLog
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("itemInSession", "item_in_session")
        .withColumnRenamed("userAgent", "user_agent")
//    dfLogRenamed.show(10)

    val dfLogRenamedFiltered = dfLogRenamed
        .filter($"page".equalTo("NextSong"))
//    dfLogRenamedFiltered.show(10)


    // extract columns for users_table
    println("\n---users_table ----\n")
    val usersTableCols = Seq("user_id", "first_name", "last_name", "gender", "level")
    val usersTable = dfLogRenamedFiltered
      // .select(usersTableCols.map(col(_)): _*)
      .select(usersTableCols.map(col): _*)
        .dropDuplicates()
    usersTable.printSchema()
//    usersTable.show(10)
//    usersTable.describe().show()

    // write users_table to parquet files
    usersTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/users_table_parquet")


    // create timestamp column from original ts column
    println("\n ---- logDf w/ mutates ----\n")

    val getTimestamp = udf((ts: Long) => ts / 1000.0)
    val dfLogRenamedFilteredTsSec = dfLogRenamedFiltered
        .withColumn(s"tsSec", getTimestamp($"ts"))
//    dfLogRenamedFilteredTsSec.printSchema()
//    dfLogRenamedFilteredTsSec.select("ts", "tsSec").show(10)
//    dfLogRenamedFilteredTsSec.select("ts", "tsSec").describe().show()

    val dfLogRenamedFilteredTsSecDatetime = dfLogRenamedFilteredTsSec
        .withColumn("start_time", to_timestamp($"tsSec"))
//    dfLogRenamedFilteredTsSecDatetime.printSchema()
//    dfLogRenamedFilteredTsSecDatetime.select("ts", "tsSec", "start_time").show(10)
//    dfLogRenamedFilteredTsSecDatetime.select("ts", "tsSec", "start_time").describe().show()


    // extract columns to create timeTable
    println("\n---- timeTable ----\n")
    val timeTable = dfLogRenamedFilteredTsSecDatetime
        .select("start_time")
        .withColumn("hour", hour($"start_time"))
        .withColumn("day", dayofmonth($"start_time"))
        .withColumn("week", weekofyear($"start_time"))
        .withColumn("month", month($"start_time"))
        .withColumn("year", year($"start_time"))
        .withColumn("weekday", dayofweek($"start_time"))
    timeTable.printSchema()
//    timeTable.show(10)
//    timeTable.describe().show()

    // write timeTable to parquet files partitioned by year and month
    timeTable.write
        .mode("overwrite")
        .parquet(outputData + "data_lake/time_table_parquet")


    // read in song data to use for songplaysTable



    //    val jsonDirStr = "/home/hynso/SpiderOak Hive/Learning/Udacity/DataEngineering/3-DataLakesWithSpark/Lesson2/data"
    //    val jsonFileStr = "sparkify_log_small.json"
    //    val jsonPathStr = s"$jsonDirStr/$jsonFileStr"

    //    val logData = spark.read.json(jsonPathStr)

    //    logData.show()
    //    logData.printSchema()
    //    logData.select("artist").show()
    //    logData.select($"artist", $"song").show()
    //    logData.filter($"length" > 1000).show()
    //    logData.select("artist").distinct().orderBy("artist").show()
    //    logData.select("page").distinct().orderBy("page").show()
    //
    //    val multTen = udf((x: Double) => x * 10)
    //    logData
    //      .withColumn("lengthMultTen", multTen($"length"))
    //      .select($"length", $"lengthMultTen")
    //      .show()
    //
    //    val flagDowngrade = udf((pageEvent: String) => if (pageEvent == "Submit Downgrade") 1 else 0)
    //    logData
    //      .filter($"userId".equalTo("1138"))
    //      .withColumn("submitDowngrade", flagDowngrade($"page"))
    //      .select($"page", $"submitDowngrade")
    //      .show(200)

    spark.stop()

  }

}
