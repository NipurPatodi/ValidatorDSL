package com.dreambig.dsl.validate.impl

import com.dreambig.dsl.validate.impl.Fun.Fun
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.col

class Operator( name:String){


  def greaterThanValue ( value: String): Column = col(name) > value

  def greaterThanField ( value: String):Column=  col(name) < col(value)


  def smallerThanValue ( value: String): Column = col(name) < value

  def smallerThanField ( value: String):Column=  col(name) < col(value)


  def equalToValue ( value: String): Column = col(name) === value

  def equalToField ( value: String):Column= col(name) === col(value)

  def notEqualToValue(value:String) :Column =col(name) =!= value
  def notEqualToField(value:String) : Column = col(name) =!= col(value)

  def isValidZipCode:Column = col(name).rlike("^[0-9]{5}(?:-[0-9]{4})?$")

  def by(fun:Fun):ColOperator = fun match {
    case  f if f ==Fun.length || f == Fun.size  => new ColOperator(functions.length(col(name)))
  }
}

object Fun extends Enumeration {
  type Fun = Value
  val length, size = Value
}

