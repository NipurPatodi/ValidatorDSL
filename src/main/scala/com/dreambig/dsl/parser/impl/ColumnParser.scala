package com.dreambig.dsl.parser.impl

import com.dreambig.dsl.parser.Parser

import scala.collection.mutable.Stack
import scala.util.Try
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

class ColumnParser extends Parser[String, Column]{

  private val OperatorStack = Stack[String]()
  private val OperatantStack = Stack[Column]()
  private val operator = List ("||","&&",">","<",">=","<=")
  private val operatorMap =initOperatorMap()

  private def  initOperatorMap() ={
    val or =(x:Column, y:Column)=> x or y
    val and =(x:Column, y:Column)=> x and y
    val greater =(x:Column, y:Column)=> y > x
    val smaller =(x:Column, y:Column)=> y < x
    val greaterEqual =(x:Column, y:Column)=> y >= x
    val smallerEqual =(x:Column, y:Column)=> y <= x
    val equal =(x:Column, y:Column)=> x === y
    Map ("||"->or,"&&"->and ,">"->greater,"<"->smaller,">="->greaterEqual,"<="->smallerEqual,"=="->equal)
  }

  private def operate(operatantStack:Stack[Column],operatorStack: Stack[String],operatorMap:Map[String,(Column,Column)=>Column])={
    val op1 = operatantStack.pop()
    val op2 = operatantStack.pop()
    val opr = operatorStack.pop()
    operatorStack.pop // poping '('
    val res = operatorMap(opr)(op1,op2)
    operatantStack.push(res)
  }

  override def parse(input: String): Try[Column] = {
    Try {
      val token = input.split(" +")

      token.foreach(
        _ match {
          case "(" => OperatorStack.push("(")
          case ")" =>
            operate(OperatantStack, OperatorStack, operatorMap)
          case x if operator contains x => OperatorStack.push(x)
          case y => OperatantStack.push(col(y))
        }
      )
      OperatantStack.pop
    }
}

}
