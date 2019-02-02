package com.dreambig.dsl.parser.impl

import com.dreambig.dsl.parser.validate.Validate
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.scalatest.matchers.Matcher

class ColumnParserTest  extends FlatSpec with BeforeAndAfter with SharedSparkContext {

   lazy  val ss = SparkSession.builder().appName("Test").master("local").getOrCreate()

  var inputDf:DataFrame =_

  before(inputDf =ss.read.option("delimiter",";").option("header","true").csv(this.getClass.getResource("/Car").getFile))



  "A GreaterThan operator" should " match value and fields" in  {
    val condition1 =  Validate field "id" greaterThanValue  "3"
    val res1 =inputDf.filter(condition1)
    assertResult(1)(res1.count())
    assertResult("4")(res1.select("id").collect.head(0))

    val condition2 =  Validate field "cc" greaterThanField  "id"
    val res2 =inputDf.filter(condition2)
    assertResult(3)(res2.count())
    assertResult("2")(res2.select("id").collect.head(0))

  }





}
