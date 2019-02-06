package com.dreambig.dsl.validate.impl

import org.apache.spark.sql.Column


class ColOperator(column: Column){

  def greaterThan (value:String):Column = column > value
}