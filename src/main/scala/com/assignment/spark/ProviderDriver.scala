package com.assignment.spark

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ProviderDriver {
  case class provider(provider_id:String,provider_specialty:String,first_name:String,middle_name:String,last_name:String)
  case class visits(visit_id:String,provider_id:String,visit_date:String)
  val providerFile="E:\\Assignment\\providers.csv"
  val visitFile="E:\\Assignment\\visits.csv"
  def main(args: Array[String]): Unit = {
    val appName = "Provide Insights"
    System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.1")
    //val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    //val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate
    val sc = spark.sparkContext
    val providerRDD=sc.textFile(providerFile)
    val header = providerRDD.first()
    val filterProviderRDD=providerRDD.filter(row => row!=header)
    filterProviderRDD.foreach(x=>println(x))
    val visitRDD=sc.textFile(visitFile)
    val splitProviderRDD=filterProviderRDD.map( x => x.split('|')).
      map(row => (row(0),provider(row(0),row(1),row(2),row(3),row(4))))
    val splitVisitRDD=visitRDD.map(x=>x.split(',')).map(row=>(row(1),visits(row(0),row(1),row(2))))
    val providerDF=spark.createDataFrame(splitProviderRDD)
    val visitsDF=spark.createDataFrame(splitVisitRDD)
    providerDF.show(10)
    visitsDF.show(10)
    /** val joinRDD=splitProviderRDD.join(splitVisitRDD)
    val mapNumDF = spark.createDataFrame(joinRDD)
    val reduceKeyDF =mapNumDF.groupBy("provider_id").count()
    val namesRDD = splitProviderRDD.map(x => (x._1, (x._2.first_name + " ", x._2.middle_name + " " + x._2.last_name)))
    val sparkDF=spark.createDataFrame(namesRDD)
    val providerDF=sparkDF.withColumnRenamed("_1","provider_id").withColumnRenamed("_2","Full Name")
    //val countsRawDF=spark.createDataFrame(reduceKeyRDD)
    //val countsDF=countsRawDF.withColumnRenamed("_1","provider_id").withColumnRenamed("_2","Count")

    reduceKeyDF.show()
    providerDF.show()*/



  }

}
