package com.dreambig.dsl.parser.impl


import org.scalatest.FlatSpec

import scala.util.Success

class StringParserTest extends FlatSpec {

  "A Parser" should " parse Prefix well" in {
    val parser = new StringParser
    assertResult(Success(true))(parser.parse("(  ( 1 > 0 )  ||  ( 2 < 0 ) )"))
  }

}
