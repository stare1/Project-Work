package com.ctm

import scala.util.{Try, Success, Failure}
import org.apache.spark.{TaskContext, SparkConf}
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import java.nio.file.{Paths, Files}

object Main {

    val usage = """
        Jar File <chart/state_chart> <number of Chart Tracks> <Input Filename Path> <Output Path>
    """

    def isValidAction(arg: String): Boolean = {
        val lowerArg = arg.toLowerCase
        lowerArg == "chart" || lowerArg == "state_chart"
    }

    def isValidNumTracks(arg: String): Boolean = Try(arg.toInt).isSuccess

    def isValidInputFile(arg: String): Boolean = {
        Files.exists(Paths.get(arg))
    }

    def isValidOutputPath(arg: String): Boolean = {
        Files.exists(Paths.get(arg))
    }

        // Top Chart Tracks

    def topChart(sqlContext: org.apache.spark.sql.SQLContext, 
                limit: Int,
                outputPath: String,
                df: org.apache.spark.sql.DataFrame) = {

        import sqlContext._
        import sqlContext.implicits._
        import org.apache.spark.sql.expressions.Window
        import org.apache.spark.sql._        
        import org.apache.spark.sql.functions._

        val byCount = Window.orderBy('count.desc)

        val topList = df.select("timezone", 
                        "match.track.id",
                        "match.track.metadata.artistname",
                        "match.track.metadata.tracktitle")
                .rdd
                .map(x => ( (x(1).toString, x(2).toString, x(3).toString), 1) )
                .reduceByKey(_ + _)
                .map(x => (x._2, x._1._3, x._1._2))
                .toDF("count", "Title Track", "Artist Name")

        val topRankNum = topList.withColumn("dense_rank", dense_rank over byCount)
        val topChartNum = topRankNum.filter((s"dense_rank<=$limit"))

        topChartNum.select("dense_rank", "Title Track", "Artist Name")
           .toDF("CHART POSITION", "TITLE TRACK", "ARTIST NAME")
           .coalesce(1)
           .write
           .format("com.databricks.spark.csv")
           .mode("overwrite")
           .option("header", "true")
           .save(s"$outputPath/TopChart")

    }

        // Top Chart Tracks Statewise

    def topStateChart(sqlContext: org.apache.spark.sql.SQLContext,
                      limit: Int,
                      outputPath: String,
                      df: org.apache.spark.sql.DataFrame) = {
        
        import sqlContext._
        import sqlContext.implicits._
        import org.apache.spark.sql.expressions.Window
        import org.apache.spark.sql._
        import org.apache.spark.sql.functions._

        val df1 = df.select("timezone",
                            "match.track.id", 
                            "match.track.metadata.artistname", 
                            "match.track.metadata.tracktitle")
                    .withColumn("country", split(col("timezone"), "/").getItem(0))
                    .withColumn("state", split(col("timezone"), "/").getItem(1))
                    .select("id", "artistname", "tracktitle", "country",  "state")
                    .filter(col("country").equalTo("America") || col("country").equalTo("US"))


        case class Tag(id: String, artistName: String, trackTitle: String, country: String, state: String)

        val df2 = df1.rdd.map(x => (   (x(0).toString,
                                        x(1).toString, 
                                        x(2).toString, 
                                        x(3).toString, 
                                        x(4).toString),
                                    1))
                        .reduceByKey(_ + _)
                        .map(x => (x._1, x._1._5, x._2))
                        .toDF("tag", "state", "count")


        val byState = Window.partitionBy('state).orderBy('count.desc)
        val df3 = df2.withColumn("dense_rank", dense_rank over byState)
        val df4 = df3.filter((s"dense_rank<=$limit"))

        df4.select("dense_rank", "state", "tag._2", "tag._3")
           .toDF("CHART POSITION", "STATE", "ARTIST NAME", "TITLE TRACK")
           .coalesce(1)
           .write
           .format("com.databricks.spark.csv")
           .mode("overwrite")
           .option("header", "true")
           .save(s"$outputPath/TopStateChart")

}

    def main(args: Array[String]): Unit = {

        if (args.length < 4) { 
            println(usage)
            sys.exit(1)
        }

        val chartType = args(0)
        val limit = args(1)
        val inputFile = args(2)
        val outputPath = args(3)
        
        if (!isValidAction(chartType) || 
            !isValidNumTracks(limit)) {
            println("Input Not Valid, Please check the Usage.")
            println(usage)
            sys.exit(1)
        }

        if (!isValidInputFile(inputFile)) {
            println("It seems, the Input File doesn't exist, please check the path and filename.")
            sys.exit(1)
        }

        if (!isValidOutputPath(outputPath)) {
            println("It seems, Output Path doesn't exist, please check the path and directory name.")
            sys.exit(1)
        }

        val conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val df = sqlContext.read.json(inputFile)
       // val df = sqlContext.read.json("/Users/stare/Downloads/shazamtagdata.json")
       // val df = sqlContext.read.json("/Users/stare/Downloads/x.json")
        
        chartType match {
            case "chart" => topChart(sqlContext, limit.toInt, outputPath, df)

            case "state_chart" => topStateChart(sqlContext, limit.toInt, outputPath, df)

            case _ => {
                        println(usage)
                        sys.exit(1)
                      }
        }

}
}