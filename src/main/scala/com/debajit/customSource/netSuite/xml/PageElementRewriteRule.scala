package com.debajit.customSource.netSuite.xml

import scala.xml._
import scala.xml.transform._

/**
 * @author debajit
 */
class PageElementRewriteRule(elementValue: String) extends RewriteRule {
  override def transform(n: Node): Seq[Node] = n match {
    case Elem(prefix, "Page", attribs, scope, _*) =>
      Elem(prefix, "Page", attribs, scope, Text(elementValue))
    case other => other
  }
}
