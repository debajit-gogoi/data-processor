package com.debajit.customSource.netSuite.ws

import com.debajit.customSource.netSuite.model.NetSuiteInput
import com.debajit.customSource.netSuite.xml.NetSuiteElement
import com.typesafe.scalalogging.LazyLogging
import org.springframework.ws.client.core.WebServiceTemplate
import org.springframework.ws.soap.axiom.AxiomSoapMessageFactory

import java.io.{StringReader, StringWriter}
import javax.xml.transform.stream.{StreamResult, StreamSource}

/**
 * @author debajit
 */
class NetSuiteClient(
                      val netSuiteInput: NetSuiteInput
                    ) extends LazyLogging {

  private val namespaceMsg2016 = "urn:messages_2016_1.platform.webservices.netsuite.com"
  private val prefixMsg2016 = "smlm"

  private val searchIdElement = "searchId"
  private val pageIndexElement = "pageIndex"
  private val searchMoreWithIdElement = "searchMoreWithId"

  private val netSuiteEndpoint = "https://webservices.netsuite.com/services/NetSuitePort_2016_1"
  //  private val netSuiteEndpoint = "http://localhost:3031/netsuite/service"
  private val webServiceTemplate = createWebServiceTemplate

  def search(): String = {
    execute(soapHeaderHandler("search"), netSuiteInput.request)
  }

  def searchMoreWithId(searchId: String, pageIndex: Long): String = {
    execute(soapHeaderHandler("searchMoreWithId"), searchMoreWithIdRequest(searchId, pageIndex))
  }

  private def searchMoreWithIdRequest(searchId: String, pageIndex: Long): String = {
    val searchIdElem = NetSuiteElement(prefixMsg2016, searchIdElement,
      namespaceMsg2016, null, searchId, null)
    val pageIndexElem = NetSuiteElement(prefixMsg2016, pageIndexElement,
      namespaceMsg2016, null, pageIndex.toString, null)

    val searchMoreWithIdElem = NetSuiteElement(prefixMsg2016, searchMoreWithIdElement,
      namespaceMsg2016, null, null, List(searchIdElem, pageIndexElem))

    searchMoreWithIdElem.toString
  }

  private def execute(soapHeaderHandler: SoapHeaderHandler, request: String): String = {
    logger.debug("Request : " + request)
    val source = new StreamSource(new StringReader(request))
    val writer = new StringWriter
    val streamResult = new StreamResult(writer)
    webServiceTemplate.sendSourceAndReceiveToResult(source, soapHeaderHandler, streamResult)

    val response = writer.toString
    logger.debug("Response : " + response)

    return response
  }

  private def soapHeaderHandler(soapHeader: String): SoapHeaderHandler = {
    new SoapHeaderHandler(netSuiteInput, soapHeader)
  }

  private def createWebServiceTemplate: WebServiceTemplate = {
    val wsTemplate = new WebServiceTemplate(new AxiomSoapMessageFactory)
    wsTemplate.setDefaultUri(netSuiteEndpoint)

    wsTemplate
  }
}
