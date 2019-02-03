package com.dreambig.dsl.validate.impl

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

trait Validable { val validate= Validate}

object Validate{

  def field (name: String) = new Operator(name)
}









