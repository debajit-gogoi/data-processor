package com.debajit.dataProcessor.structs

/**
 * @author debajit
 */
case class LogMessage(appName: String, appId: String, errorMessage: String, stackTrace: String, hostName: String)
