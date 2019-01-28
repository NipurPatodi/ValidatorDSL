package com.dreambig.dsl.validate.util

import scala.collection.mutable.Stack

object ExpressionUtil {

  private val operator =List(">","<",">=","<=","==","&&","||")

   def isValid(input:String):Boolean={
    val stack = Stack[String]()
    input.toCharArray.foreach(x=> if(x =='(') stack.push(x.toString) else if(x == ')') stack.pop())
    stack.isEmpty
  }

  def format(input:String):String ={
    val buffer =  new StringBuilder()
    for (token <- input.toCharArray){
      if(token == '(' || token ==')'){
        buffer.append(" ")
        buffer.append( token )
        buffer.append(" ")
      }else{
        buffer.append( token )
      }
    }

    buffer.toString.trim
  }

}
