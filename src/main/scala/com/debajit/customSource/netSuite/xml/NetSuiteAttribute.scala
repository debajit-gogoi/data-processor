package com.debajit.customSource.netSuite.xml

/**
 * @author debajit
 */
case class NetSuiteAttribute(
                              localName: String,
                              value: String
                            ) {

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append(localName)
    sb.append("=")
    if (value != null) {
      sb.append("\"").append(value).append("\"")
    }

    sb.toString()
  }

}
