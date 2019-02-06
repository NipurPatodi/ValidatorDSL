package com.dreambig.dsl.parser.impl

import com.dreambig.dsl.parser.Parser
import org.apache.spark.sql.Column

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.Try


class DslParser extends Parser[String, Column] {

  private val toolbox = currentMirror.mkToolBox()
  private val template = " import com.dreambig.dsl.validate.impl._ \n import com.dreambig.dsl.validate.impl.Fun._ \n #"


  override def parse(input: String): Try[Column] = {
    Try {
      toolbox.eval(toolbox.parse(template.replace("#", input))).asInstanceOf[Column]
    }

  }

}
