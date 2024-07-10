package com.assignment.spark
import org.apache.spark.sql.SparkSession

object ProviderInsight extends App{
  System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Provider Insights")
    .getOrCreate();


  case class Member_Eligibity(member_id:String,first_name:String,middle_name:String,last_name:String)
  case class Member_Months(eligbility_number:String,member_id:String,eligiblity_effective_date:String,eligibility_member_month:String)
  val memEligibleDF=spark.read.format("com.crealytics.spark.excel").option("useHeader", "true")
    .load("F:\\SparkAssignment\\member_eligibility.xlsx")
  val memMontsDF=spark.read.format("com.crealytics.spark.excel").option("useHeader", "true")
    .load("F:\\SparkAssignment\\member_months.xlsx")
  import spark.implicits._
  val memEligibleMapDF=memEligibleDF.as[Member_Eligibity]
  val memMonthsMapDF=memMontsDF.as[Member_Months]
  val membJoin=memEligibleMapDF.join(memMonthsMapDF,(memEligibleMapDF("member_id")===memMonthsMapDF("member_id")),"inner")
    .drop(memMonthsMapDF("member_id")).drop(memMonthsMapDF("eligbility_number"))
  val memberGrpdf=membJoin.groupBy("member_id").count()
  val finalResultsDf=memEligibleMapDF.join(memberGrpdf,(memEligibleMapDF("member_id")===memberGrpdf("member_id")),
      "inner")
    .drop(membJoin("member_id"))
  val namesDF=finalResultsDf.rdd.map(row=>(row.getString(3),row.getString(0)+" "+row.getString(1)+" "+row.getString(2))).toDF()
  finalResultsDf.show(20)
  val outputDf=finalResultsDf.join(namesDF,(finalResultsDf("member_id")===namesDF("_1")),"inner")
    .drop(namesDF("_1")).drop(finalResultsDf("first_name")).drop(finalResultsDf("middle_name"))
    .drop(finalResultsDf("last_name")).withColumnRenamed("_2","Full Name")

  outputDf.write.partitionBy("member_id").json("F:\\SparkAssignment\\OutputMembers1")
  outputDf.show(10)
  import org.apache.spark.sql.functions._
 // val getConcatenated = udf( (first: String, second: String) => { first + " " + second } )

}
