import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
$ sbt package
$ $SPARK_HOME/bin/spark-submit --class SimpleApp --master local[*] target/scala-2.11/sparksandbox_2.11-0.1.jar
 */

object SimpleApp {

  def main(args: Array[String]): Unit = {

    /*
     * Define functions
     */

    def createSparkSession(awsAccessKeyId: String, awsSecretAccessKey: String): SparkSession = {
      println("Creating Spark session...")
      val spark: SparkSession = SparkSession
        .builder
        .appName("Sandbox Application")
        // .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5")
        .config("fs.s3a.access.key", awsAccessKeyId)
        .config("fs.s3a.secret.key", awsSecretAccessKey)
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        // .config("fs.s3.awsAccessKeyId", awsAccessKeyId)
        // .config("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
        .master("local[*]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
    }

    def loadSongData(spark: SparkSession, inputBucket: String): DataFrame = {
      // get filepath to songData json files
      val songData: String = inputBucket + "song_data/A/A/*/*.json"
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
      // read song data json files
      spark.read
        .schema(songDataSchema)
        .json(songData)
    }

    def loadLogData(spark: SparkSession, inputBucket: String): DataFrame = {
      // get filepath to logData json files
      val logData: String = inputBucket + "log_data/*/*/*.json"
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
      // define udf for adjusting millisecond epoch timestamp
      val divideBy1000 = udf((ts: Long) => ts / 1000.0)
      // read log data json files
      spark.read
        .schema(logDataSchema)
        .json(logData)
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("itemInSession", "item_in_session")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
        .filter(col("page").equalTo("NextSong"))
        .withColumn("tsSec", divideBy1000(col("ts")))
        .withColumn("start_time", to_timestamp(col("tsSec")))
        .drop("tsSec")
    }

    def createDimensionTable(sourceDF: DataFrame, selectedCols: Seq[String]): DataFrame = {
      sourceDF
        .select(selectedCols.map(s => col(s)): _*)
        .dropDuplicates()
    }

    def createTimeTable(logData: DataFrame): DataFrame = {
      // extract and transform "start_time" column
      logData
        .select("start_time")
        .dropDuplicates()
        .withColumn("hour", hour(col("start_time")))
        .withColumn("day", dayofmonth(col("start_time")))
        .withColumn("week", weekofyear(col("start_time")))
        .withColumn("month", month(col("start_time")))
        .withColumn("year", year(col("start_time")))
        .withColumn("weekday", dayofweek(col("start_time")))
    }

    /*
     * Set up app
     */

    // val awsConfig = ConfigFactory.load("aws")
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))

    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")

    val inputData = "s3a://udacity-dend/"
    val outputData = "s3a://ldnicolasmay-bucket/"

    // Get Spark session
    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    /*
     * Load raw data
     */

    // Load song data
    println("Loading raw song data...")
    val songDF: DataFrame = loadSongData(spark, inputData)

    // Load log data
    println("Loading raw log data...")
    val logDF: DataFrame = loadLogData(spark, inputData)

    /*
     * Create dimension tables
     */

    // create songs table
    println("Creating songs table...")
    val songsTableCols: Seq[String] =
      Seq("song_id", "title", "artist_id", "artist_name", "year", "duration")
    val songsTable: DataFrame = createDimensionTable(songDF, songsTableCols)

    // create artists table
    println("Creating artists table...")
    val artistsTableCols: Seq[String] =
      Seq("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    val artistsTable: DataFrame = createDimensionTable(songDF, artistsTableCols)

    // create users table
    println("Creating users table...")
    val usersTableCols: Seq[String] =
      Seq("user_id", "first_name", "last_name", "gender", "level")
    val usersTable: DataFrame = createDimensionTable(logDF, usersTableCols)

    // create time table
    println("Creating time table...")
    val timeTable: DataFrame = createTimeTable(logDF)


    /*
     * Create fact table
     */

    // create songplays table from
    println("Creating songplays table...")
    val songplaysTable = logDF
      .withColumnRenamed("song", "title")
      .withColumnRenamed("artist", "artist_name")
      .withColumnRenamed("length", "duration")
      .select(
        "start_time",
        "user_id",
        "level",
        "session_id",
        "location",
        "user_agent",
        "title",
        "artist_name",
        "duration")
      .join(songsTable, Seq("title", "artist_name", "duration"))
      .drop("title", "artist_name", "duration")
      .withColumn("songplay_id", monotonically_increasing_id())


    /*
     * Write dimension tables to parquet on S3
     */

    // write songs table
    println("Writing songs table...")
    songsTable.write
      .mode("overwrite")
      .partitionBy("year", "artist_id")
      .parquet(outputData + "data_lake/songs_table_parquet")

    // write artists table
    println("Writing artists table...")
    artistsTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/artists_table_parquet")

    // write users table
    println("Writing users table...")
    usersTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/users_table_parquet")

    // write time table
    println("Writing time table...")
    timeTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/time_table_parquet")

    /*
     * Write fact table to parquet on S3
     */

    // write songplays table
    println("Writing songplays table...")
    songplaysTable.write
      .mode("overwrite")
      .parquet(outputData + "data_lake/songplays_table_parquet")


    spark.stop()

  }

}
