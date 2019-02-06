package com.dreambig.dsl.parser.impl

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ConfigParseTest extends FlatSpec with BeforeAndAfter with SharedSparkContext{


  lazy val ss:SparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()
  var inputDf:DataFrame =_
  var config:Config=_

  before{
    inputDf =ss.read.option("delimiter",";").option("header","true").csv(this.getClass.getResource("/Car").getFile)
    val configFile:File = new File(this.getClass.getResource("/Rules.conf").getFile)
    config = ConfigFactory.parseFile(configFile)

  }

  "Rule1" should "ok" in{
    val condition =config.getString("config.validation.rule1").trim
    val parser = new DslParser
    val res =  parser.parse(condition).get
    assert(res != null)
    assertResult(1)(inputDf.filter(res).count())
  }

  "Rule2" should "ok" in{
    val condition =config.getString("config.validation.rule2").trim
    val parser = new DslParser
    val res =  parser.parse(condition).get
    assert(res != null)
    assertResult(2)(inputDf.filter(res).count())
  }

  "Rule3" should "ok" in{
    val condition =config.getString("config.validation.rule3").trim
    val parser = new DslParser
    val res =  parser.parse(condition).get
    assert(res != null)
    assertResult(1)(inputDf.filter(res).count())
  }
}
