package com.assignment.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object MemberDetails extends App {
  System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.1")
  val conf = new SparkConf().setMaster("local[2]").setAppName("Member Count")
  val sc = new SparkContext(conf)

  val memberFileRDD = sc.textFile("F:\\SparkAssignment\\member_eligibility.csv")
  val memberDeteFileRDD = sc.textFile("F:\\SparkAssignment\\member_months.csv")

  case class Member_Eligibity(member_id: String, first_name: String, middle_name: String, last_name: String)

  case class Member_Months(eligbility_number: String, member_id: String, eligiblity_effective_date: String, eligibility_member_month: String)

  val mapMemberRDD = memberFileRDD.map(x => x.split(",")).
    map(row => (row(0), Member_Eligibity(row(0), row(1), row(2), row(3))))
  val mapMemDetRDD = memberDeteFileRDD.map(x => x.split(",")).map(row => (row(1), Member_Months(row(0), row(1), row(2), row(3))))
  mapMemDetRDD.foreach(x=>println(x))
  val joinedRDD = mapMemberRDD.join(mapMemDetRDD)
  //val groupedRDD=joinedRDD.groupByKey()
  val mapNumRDD = joinedRDD.map(x => (x._1, 1L))
  val reduceKeyRDD = mapNumRDD.reduceByKey(_ + _)
  val namesRDD = mapMemberRDD.map(x => (x._1, (x._2.first_name + " ", x._2.middle_name + " " + x._2.last_name)))
  //reduceKeyRDD.foreach(x=>println(x))
  val finalRDD = namesRDD.join(reduceKeyRDD)
  val partRDD = finalRDD.partitionBy(new HashPartitioner(5))
  //val jsonRDD=partRDD.map(x=>("{\"member_id\":\""+x._1+"\", \"name\":\""+(x._2._1._1+" "+x._2._1._2)+"\",\"count\":"+x._2._2+"}"))
  val jsonRDD = partRDD.map(x => (x._1 + "," + (x._2._1._1 + " " + x._2._1._2) + "," + x._2._2))
  jsonRDD.repartition(1).saveAsTextFile("F:\\SparkAssignment\\OutputRDD4\\members.json")


}
