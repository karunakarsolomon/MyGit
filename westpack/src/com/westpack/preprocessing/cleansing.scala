package com.westpack.preprocessing

import org.apache.spark.sql.SparkSession


object cleansing {
  
  def main(args:Array[String]){
        // added command
       
    val sc = SparkSession.builder().master("local").appName("SparkSessionZipsExample").getOrCreate()
    var filePath = "E:\\Karuna\\Hadoop\\input.txt"
    val df = sc.read.text(filePath).toDF
    df.show
    println("Preprocessing ......")
     
  }
}