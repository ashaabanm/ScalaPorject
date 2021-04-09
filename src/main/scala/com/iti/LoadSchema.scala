package com.iti

import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


class LoadSchema {
  def loadRules(spark:SparkSession,rulesPath:String):DataFrame ={

    val struct = StructType(
      StructField("AppId", IntegerType, true) ::
        StructField("AppName", DataTypes.StringType,false) ::
        StructField("StartHour", IntegerType, false) ::
        StructField("EndHour", IntegerType, false) ::
        StructField("TrafficVolume", IntegerType, false) :: Nil)

    val segment = spark.read.schema(struct)
      .option("delimiter",",")
      .option("header", "true")
      .csv(rulesPath)
    return  segment
  }

  def loadSegment(spark:SparkSession,segmentPath:String):DataFrame ={

    val struct = StructType(
      StructField("CustId", DataTypes.StringType, true)  :: Nil)

    val segment = spark.read.schema(struct)
      .option("delimiter"," ")
      .option("header", "true")
      .csv(segmentPath)
    return  segment
  }
}
