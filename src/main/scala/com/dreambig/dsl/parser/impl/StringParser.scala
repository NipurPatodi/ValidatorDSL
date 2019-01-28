package com.dreambig.dsl.parser.impl

import com.dreambig.dsl.parser.Parser

import scala.collection.mutable.Stack
import scala.util.Try

class StringParser extends Parser[String, Boolean]{

  private val OperatorStack = Stack[String]()
  private val OperatantStack = Stack[String]()
  private val operator = List ("||","&&",">","<",">=","<=")
  private val operatorMap =initOperatorMap()

  private def  initOperatorMap() ={
    val or =(x:String, y:String)=> x.toBoolean || y.toBoolean
    val and =(x:String, y:String)=> x.toBoolean && y.toBoolean
    val greater =(x:String, y:String)=> y.toInt> x.toInt
    val smaller =(x:String, y:String)=> y.toInt< x.toInt
    val greaterEqual =(x:String, y:String)=> y.toInt>=x.toInt
    val smallerEqual =(x:String, y:String)=> y.toInt<=x.toInt
    val equal =(x:String, y:String)=> x.toInt==y.toInt
    Map ("||"->or,"&&"->and ,">"->greater,"<"->smaller,">="->greaterEqual,"<="->smallerEqual,"=="->equal)
  }

  private def operate(operatantStack:Stack[String],operatorStack: Stack[String],operatorMap:Map[String,(String,String)=>Boolean])={
    val op1 = operatantStack.pop()
    val op2 = operatantStack.pop()
    val opr = operatorStack.pop()
    operatorStack.pop // poping '('
    val res = operatorMap(opr)(op1,op2)
    operatantStack.push(res.toString)
  }

  override def parse(input: String): Try[Boolean] = {
    Try {
      val token = input.split(" +")

      token.foreach(
        _ match {
          case "(" => OperatorStack.push("(")
          case ")" =>
            operate(OperatantStack, OperatorStack, operatorMap)
          case x if operator contains x => OperatorStack.push(x)
          case y => OperatantStack.push(y)
        }
      )
      OperatantStack.pop.toBoolean
    }
}

}
