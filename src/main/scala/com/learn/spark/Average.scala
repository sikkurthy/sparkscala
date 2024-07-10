package com.learn.spark


import org.apache.spark.sql.SparkSession

object Average extends App{

  System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Average Calc")
    .getOrCreate();

  val intMarks=spark.sparkContext.textFile("F:\\Temp\\SparkInputFiles\\marks.csv")
  var list_mrks=intMarks.map(x => (x.split(',')(0),List(x.split(',')(1).toFloat,x.split(',')(2).toFloat,x.split(',')(3).toFloat,x.split(',')(4).toFloat,x.split(',')(5).toFloat,x.split(',')(6).toFloat)))
  var per_mrks=list_mrks.mapValues(x => x.sum/x.length)
  //per_mrks.collect()
  per_mrks.saveAsTextFile("F:\\Temp\\SparkOutput\\AvgMarksNew1\\")

}
