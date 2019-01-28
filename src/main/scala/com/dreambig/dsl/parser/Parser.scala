package com.dreambig.dsl.parser

import scala.util.Try

trait Parser [I,O] {

  /***
    * Skeleton method to parse input type to output type
    * @param input I
    * @return O
    */
  def parse (input:I):Try[O]
}
