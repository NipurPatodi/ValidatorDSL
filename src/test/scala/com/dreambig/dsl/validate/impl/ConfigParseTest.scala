package com.dreambig.dsl.validate.impl

import java.io.File

import com.dreambig.dsl.parser.impl.DslParser
import com.holdenkarau.spark.testing.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ConfigParseTest extends FlatSpec with BeforeAndAfter with SharedSparkContext{


  lazy val ss:SparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()
  var inputDf:DataFrame =_

  before{
    inputDf =ss.read.option("delimiter",";").option("header","true").csv(this.getClass.getResource("/Car").getFile)
  }

  "Test" should "ok" in{
    val configFile:File = new File(this.getClass.getResource("/Test.conf").getFile)
    val config = ConfigFactory.parseFile(configFile)
    val condition =config.getString("config.validation.rule").trim
    val parser = new DslParser
    val res =  parser.parse(condition).get
    assert(res != null)
    inputDf.filter(res).show()
  }
}
