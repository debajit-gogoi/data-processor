package com.debajit.customSource.netSuite.model

/**
 * @author debajit
 */
class NetSuiteInput(
                     val email: String,
                     val password: String,
                     val account: String,
                     val role: String,
                     val applicationId: String,
                     val request: String,
                     val pageSize: Integer
                   ) {

}
