package com.dreambig.dsl.validate.impl
import com.dreambig.dsl.validate.impl.Fun._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ValidateTest  extends FlatSpec with BeforeAndAfter with SharedSparkContext {

  lazy val ss:SparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()
  var inputDf:DataFrame =_

  before{
     inputDf =ss.read.option("delimiter",";").option("header","true").csv(this.getClass.getResource("/Car").getFile)
  }



  "A GreaterThan operator" should " match value and fields" in  {
    val condition1 =  Validate field "id" greaterThanValue  "3"
    val res1 =inputDf.filter(condition1)
    assertResult(1)(res1.count())
    assertResult("4")(res1.select("id").collect.head(0))

    val condition2 =  (Validate field "cc" greaterThanField  "id") and (Validate field "cc" greaterThanField  "id") or (Validate field "cc" greaterThanField  "id")
    val res2 =inputDf.filter(condition2)
    assertResult(2)(res2.count())
    assertResult("2")(res2.select("id").collect.head(0))
}

  "Equal operators" should " match value and fields" in {

    val condition3 =  (Validate field "id" equalToValue "0") or (Validate field "cc" equalToValue   "900")
    assertResult(2)(inputDf.filter(condition3).count())

    val condition4 = Validate field "id" notEqualToValue "5"
    assertResult(4)(inputDf.filter(condition4).count())

  }


  "A length operator" should " match fields length" in  {
    val condition1 =  Validate  field "brand" by length greaterThan "4"
    val res1 =inputDf.filter(condition1)
    assertResult(3)(res1.count())
  }

  "A validate zipcode operator" should " match zipcode" in  {
    val condition1 =  Validate  field "zip" isValidZipCode
    val res1 =inputDf.filter(condition1)
    assertResult(3)(res1.count())
  }

}
