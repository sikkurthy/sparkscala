package com.assignment.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date

/**
 * Driver class to extract, transform and store data
 */
object ProviderData {

  /**
   * Case class to preserve schema
   *
   * @param provider_id
   * @param provider_specialty
   * @param full_name
   */
  case class provider(provider_id: String, provider_specialty: String, full_name: String)

  /**
   * Case class to preserve schema
   *
   * @param visit_id
   * @param provider_id
   * @param visit_date
   */
  case class visits(visit_id: String, provider_id: String, visit_date: String)

  //Assigning values to input and output paths, master and app name
  val providerFile = "E:\\Assignment\\providers.csv"
  val visitFile = "E:\\Assignment\\visits.csv"
  val master = "local[1]"
  val appName = "Provider Insights"
  val outputProvFile = "E:\\SparkAssignment\\ProvidersPartVisit\\"
  val outputVisitile = "E:\\SparkAssignment\\ProvidersMonthVisit\\"

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.1")
    val spark = SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate();
    //Set logger level to info to capture details into logger
    spark.sparkContext.setLogLevel("INFO")
    try {
      val visitsRDD = spark.sparkContext.textFile(visitFile)
      //File is CSV separated and we are splitting with , here.
      val mapMemberRDD = visitsRDD.map(x => x.split(",")).
        map(row => (visits(row(0), row(1), row(2))))
      val providerRDD = spark.sparkContext.textFile(providerFile)
      //Header needs to be removed to avoid it in processing as data
      val header = providerRDD.first()
      val filterProviderRDD = providerRDD.filter(row => row != header)
      //Since provider data is split with | we are splitting here
      val splitProviderRDD = filterProviderRDD.map(x => x.split('|'))
        .map(row => (provider(row(0), row(1), row(2) + " " + row(3) + " " + row(4))))
      import spark.implicits._
      //converting RDD to data frame with schema to perform additional operations
      val providerDF = spark.createDataFrame(splitProviderRDD).as[provider]
      val visitMapDF = spark.createDataFrame(mapMemberRDD).as[visits]
      //Join the data using provider_id to combine Provider and Visitor data.
      val provVisitJoinDF = providerDF.join(visitMapDF, (providerDF("provider_id") ===
          visitMapDF("provider_id")), "inner")
        .drop(visitMapDF("provider_id"))
      //Counting the number of visits by provider_id
      val memberGrpdf = provVisitJoinDF.groupBy("provider_id").count()
      //remove columns which are not needed
      val specialityDF = provVisitJoinDF.drop("visit_id").drop("visit_date")
      val finalResultsDf = memberGrpdf.join(specialityDF, (specialityDF("provider_id") ===
        memberGrpdf("provider_id")), "inner").drop(specialityDF("provider_id"))
      //filter by applying distinct to avoid duplicates
      //partition by provider_specialty and store the data into single json file for each partition.
      finalResultsDf.distinct().coalesce(1).write.partitionBy("provider_specialty").json(outputProvFile)
      val df2 = provVisitJoinDF.withColumn("month", date_format(to_date($"visit_date", "yyyy-MM-dd"), "MMMM"))
      //writing data into json with grouping on provider_id and month
      df2.groupBy("provider_id", "month").count().coalesce(1).write.json(outputVisitile)
    }
    catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
      // Just adding basic error message, we can add additional exceptional handling if needed
    } finally {
      // Stopping SparkSession
      spark.stop()
    }
  }
}
