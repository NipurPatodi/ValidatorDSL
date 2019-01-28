package com.dreambig.dsl.parser.impl

import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class ColumnParserTest  extends FunSuite with BeforeAndAfter with SharedSparkContext {



  test("pretty output of DataFrame's check") {
    val sql= SQLContext.getOrCreate(sc)
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("brand", StringType),
      StructField("name", StringType),
      StructField("cc", IntegerType),
      StructField("cost", LongType)
    ))
    val df =sql.createDataFrame(sc.parallelize(Seq(Row(1,"maruti","celerio",900,400000l),
    Row(2,"maruti","alto",1000,500000l),
    Row(3,"maruti","baleno",1200,900000l)
    )), schema)

    df.show()

  }

}
