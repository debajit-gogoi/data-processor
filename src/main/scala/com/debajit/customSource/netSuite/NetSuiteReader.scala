package com.debajit.customSource.netSuite

import com.debajit.customSource.netSuite.model.{NetSuiteInput, XPathInput}
import com.debajit.customSource.netSuite.util.XPathHelper
import com.debajit.customSource.netSuite.ws.NetSuiteClient
import com.debajit.java.customSource.netSuite.util.XercesWarningFilter
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
 * @author debajit
 */
class NetSuiteReader(
                      netSuiteInput: NetSuiteInput,
                      xPathInput: XPathInput
                    ) extends LazyLogging {

  val xPathHelper = new XPathHelper(xPathInput.namespaceMap, null)
  var currentPage = 1l
  var totalPages = 0l

  def read(): List[mutable.Map[String, String]] = {
    XercesWarningFilter.start()
    var records: List[mutable.Map[String, String]] = List.empty

    val nsClient = new NetSuiteClient(netSuiteInput)
    val response = nsClient.search()
    records ++= readRecords(response)

    val searchId = getSearchId(response)
    while (moreToRead(response)) {
      currentPage += 1
      logger.info("Reading page " + currentPage)
      val searchMoreResponse = nsClient.searchMoreWithId(searchId, currentPage)
      records ++= readRecords(searchMoreResponse)
    }

    records
  }

  def readRecords(nsResponse: String): List[scala.collection.mutable.Map[String, String]] = {
    logger.debug("Response from NetSuite " + nsResponse)
    var records: List[mutable.Map[String, String]] = List.empty

    if (nsResponse != null && !nsResponse.isEmpty) {
      val xmlRecords = xPathHelper.evaluate(xPathInput.recordTag, nsResponse)

      if (!xmlRecords.isEmpty) {
        records ++= read(xmlRecords)
      }
    }

    records
  }

  private def read(xmlRecords: List[String]): List[scala.collection.mutable.Map[String, String]] = {
    val recordLists = xmlRecords.map(row => read(row))
    val records = new ListBuffer[mutable.Map[String, String]]()

    for (record <- recordLists) {
      if (!record.isEmpty) {
        records += record
      }
    }

    records.toList
  }

  private def read(row: String): scala.collection.mutable.Map[String, String] = {
    logger.debug("Row : " + row)
    val record = scala.collection.mutable.Map[String, String]()
    for ((column, xpath) <- xPathInput.xpathMap) {
      val result = xPathHelper.evaluateToString(xpath, row)
      logger.debug("Xpath evaluation response for xpath " + xpath + " \n" + result)
      if (result != null && !result.isEmpty) {
        record(column) = result
      }
    }

    record
  }

  def getSearchId(nsResponse: String): String = {
    if (nsResponse == null || nsResponse.isEmpty) {
      return ""
    }

    // Reading total pages and comparing it with currentPage
    val responseXML = XML.loadString(nsResponse)
    (responseXML \\ "searchResult" \ "searchId").text
  }

  def moreToRead(nsResponse: String): Boolean = {
    if (nsResponse == null || nsResponse.isEmpty) {
      return false
    }

    // To avoid unnecessary parsing
    if (totalPages == 0l) {
      // Reading total pages and comparing it with currentPage
      val responseXML = XML.loadString(nsResponse)
      val totalPagesStr = (responseXML \\ "searchResult" \ "totalPages").text
      if (totalPagesStr != null && !totalPagesStr.isEmpty) {
        totalPages = totalPagesStr.toLong
      }

      logger.info("Total pages : " + totalPages)
    }

    logger.debug("Total Pages : " + totalPages)
    logger.debug("Current Page : " + currentPage)
    totalPages > currentPage
  }

}
