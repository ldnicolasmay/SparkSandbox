name := "SparkSandbox"

version := "0.1"

scalaVersion := "2.11.12"
// scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.5"

//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.0"

//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1"



// https://github.com/lightbend/config
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
