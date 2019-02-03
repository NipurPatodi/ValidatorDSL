package com.dreambig.dsl.validate.impl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class Operator( name:String){


  def greaterThanValue ( value: String): Column = col(name) > value

  def greaterThanField ( value: String):Column=  col(name) < col(value)


  def smallerThanValue ( value: String): Column = col(name) < value

  def smallerThanField ( value: String):Column=  col(name) < col(value)


  def equalToValue ( value: String): Column = col(name) === value

  def equalToField ( value: String):Column= col(name) === col(value)



}