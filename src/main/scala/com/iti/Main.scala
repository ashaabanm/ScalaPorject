package com.iti

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_extract, substring}

object Main {
  def main(args: Array[String]): Unit = {
    val loadSchema = new LoadSchema()
    val helper = new Helper()

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example").master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rulesDf = loadSchema.loadRules(spark,args(2))
    val segmentDf = loadSchema.loadSegment(spark,args(1)).as("segmentDf")

    val accessLines = spark.readStream
      .text(args(0))

    val regx = "(\\S+),(\\S+),(\\S+),(\\S+)"
    val dataDf = accessLines.select(
      regexp_extract(col("value"), regx, 1).alias("CustId"),
      regexp_extract(col("value"), regx, 2).alias("AppId"),
      regexp_extract(col("value"), regx, 3).alias("TotalVolume"),
      regexp_extract(col("value"), regx, 4).alias("TimeStamp"))

    val dataDf2 = dataDf.select(col("*"),
      substring(col("TimeStamp"), 9, 2).as("Hour"),
      substring(col("TimeStamp"), 0, 8).as("Date"))

    rulesDf.createOrReplaceTempView("rules")
    segmentDf.createOrReplaceTempView("segment")
    dataDf2.createOrReplaceTempView("data")

    dataDf2.printSchema()

    val dataDfSql = spark.sql("select data.CustId ,data.AppId ,AppName ,data.TotalVolume,Date,rules.TrafficVolume as AppVolumme ,TimeStamp ,Hour,rules.StartHour,rules.EndHour " +
      "from data ,segment,rules" +
      " where segment.CustId=data.CustId " +
      "and data.AppId=rules.AppId " +
      "and Hour between rules.StartHour and rules.EndHour "+
      "and data.TotalVolume>rules.TrafficVolume ")

    val query = (dataDfSql.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        helper.filterBatch(batchDF, spark,args(3))
      }.option("checkpointLocation","checkpoint/").start())

    query.awaitTermination()
    spark.stop()
  }

}
