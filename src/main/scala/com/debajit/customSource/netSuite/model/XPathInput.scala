package com.debajit.customSource.netSuite.model

/**
 * @author debajit
 */
class XPathInput(
                  val recordTag: String
                ) {

  var xpathMap: Map[String, String] = Map.empty
  var namespaceMap: Map[String, String] = Map.empty
}
