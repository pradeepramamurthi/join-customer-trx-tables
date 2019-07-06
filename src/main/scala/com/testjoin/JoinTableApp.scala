package com.testjoin

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*

  Join the two tables Customer and Transactions and find the maximum transaction amount per customer per day per month.

 */

object JoinTableApp {

  def main(args: Array[String]): Unit = {

    //Logger to keep logging only to WARN

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.WARN)


    //create SparkSession

    val spark = SparkSession.builder().appName("joinTable").master("local").getOrCreate()

    //set path

    val custPath = getClass.getClassLoader.getResource("Customer.csv").toString
    val trxPath = getClass.getClassLoader.getResource("Transactions.csv").toString


    //Create Customer and Transactions schema

    val custSchema = new StructType()
      .add("id","int")
      .add("customer_name","string")
      .add("address","string")
      .add("dob","timestamp")


    val trxSchema = new StructType()
      .add("trx_id","int")
      .add("customer_id","int")
      .add("trx_date","timestamp")
      .add("trx_amt","double")



    //create baseDF

    val custBaseDF = spark.read.option("header","true").schema(custSchema).format("csv").load(custPath)
    val trxBaseDF = spark.read.option("header","true").schema(trxSchema).format("csv").load(trxPath)


//    custBaseDF.printSchema()
//    custBaseDF.show()
//    trxBaseDF.printSchema()
//    trxBaseDF.show()

    //Filter away columns that are not required and add columns that are needed.
    val custFilteredDF = custBaseDF.drop(col("address")).drop(col("dob"))
    val trxTransformedDF = trxBaseDF.withColumn("trx_date_part",dayofmonth(col("trx_date"))).withColumn("trx_month_part",month(col("trx_date")))


    //Create Join Expression and jointype variables that will be used in the table joins.
    var joinExpr = custFilteredDF.col("id") === trxBaseDF.col("customer_id")
    var joinType = "inner"


    //Actual table join
    val jointTableDF = custFilteredDF.join(trxTransformedDF,joinExpr,joinType)

    //filter more columns as needed from joint table
    val jointFilteredDF = jointTableDF.drop(col("customer_id"))

    //jointTable.show()

    //Create a Window Spec -  A window specification defines which rows are included in the frame associated with a given input row  (Contains partitions spec, ordering spec and frame spec - here we have not used the frame spec)
    val winSpec = Window.partitionBy(col("id"),col("trx_month_part"),col("trx_date_part")).orderBy(col("trx_amt").desc)

    //With a Window Spec defined, you use Column.over operator that associates the WindowSpec with an aggregate or window function.

    val resultDF = jointFilteredDF.withColumn("max_amt",max(col("trx_amt")).over(winSpec))
                   .where(col("trx_amt") === col("max_amt"))  //filter only rows that have trx_amt = max_amt in each Window
                   .orderBy(col("trx_date").asc)


    resultDF.show()




  }

}
