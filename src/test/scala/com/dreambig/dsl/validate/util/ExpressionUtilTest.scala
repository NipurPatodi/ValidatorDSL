package com.dreambig.dsl.validate.util

import com.dreambig.dsl.parser.impl.StringParser
import org.scalatest.FlatSpec

import scala.util.Success

class ExpressionUtilTest extends FlatSpec {

  "An Expression " should " validate as true " in {
    val goodInput ="(a>b)+(c+d)"
    assertResult(true)(ExpressionUtil.isValid(goodInput))

  }

  "An Expression " should " validate as false" in {
    val badInput ="(a>b)+((c+d)"
    assertResult(false)(ExpressionUtil.isValid(badInput))
  }

  "An Expression " should " be spaced properly" in {
    val input ="(a>b)"
    assertResult("( a>b )")(ExpressionUtil.format(input))
  }
}
