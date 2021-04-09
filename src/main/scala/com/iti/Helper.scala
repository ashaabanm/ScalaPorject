package com.iti

import org.apache.spark.sql.{DataFrame, SparkSession}

class Helper {

  var oldDf: DataFrame = null

  def filterBatch(df: DataFrame, spark: SparkSession,outputPath:String) = {

    if (oldDf == null) {
      oldDf =df
      oldDf.coalesce(1)
        .write
        .partitionBy("AppName", "Date")
        .format("csv")
        .mode("append")
        .save(outputPath)
    }
    else {
      val query = df.join(oldDf, df("CustId") === oldDf("CustId") && df("AppId") === oldDf("AppId")
         && df("Date") === oldDf("Date")
        , "leftanti")

      oldDf = oldDf.union(query)

      oldDf.coalesce(1)
          .write
          .partitionBy("AppName", "Date")
          .format("csv")
          .mode("append")
          .save(outputPath)
    }

  }
}