package com.debajit.dataProcessor.utils

import com.salesforce.emp.connector.example.{BearerTokenProvider, LoggingListener}
import com.salesforce.emp.connector.{BayeuxParameters, EmpConnector, LoginHelper, TopicSubscription}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.cometd.bayeux.Channel._
import org.eclipse.jetty.util.ajax.JSON

import java.net.URL
import java.util
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import java.util.function.{Consumer, Supplier}

class SalesforceReceiver(clientName: String,
                         loginUrl: String,
                         username: String,
                         password: String,
                         topic: String,
                         replayFromOffset: Long = EmpConnector.REPLAY_FROM_EARLIEST
                        )

  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var subscriptionOpt: Option[TopicSubscription] = None

  def onStart() {
    val tokenProvider = new BearerTokenProvider(new Supplier[BayeuxParameters]() {
      override def get: BayeuxParameters = {
        try
          LoginHelper.login(new URL(loginUrl), username, password)
        catch {
          case e: Exception =>
            throw new RuntimeException(e)
        }
      }
    })

    val params = tokenProvider.login
    val connector = new EmpConnector(params)
    val loggingListener = new LoggingListener(true, true)

    connector.addListener(META_HANDSHAKE, loggingListener)
      .addListener(META_CONNECT, loggingListener)
      .addListener(META_DISCONNECT, loggingListener)
      .addListener(META_SUBSCRIBE, loggingListener)
      .addListener(META_UNSUBSCRIBE, loggingListener)

    connector.setBearerTokenProvider(tokenProvider)
    connector.start.get(5, TimeUnit.SECONDS)

    val consumer = new Consumer[util.Map[String, AnyRef]]() {
      def accept(event: util.Map[String, AnyRef]): Unit = {
        val msg = JSON.toString(event)
        logInfo(String.format("Received:\n%s", msg))
        store(msg)
      }
    }

    try {
      val subscription = connector.subscribe(topic, replayFromOffset, consumer).get(5, TimeUnit.SECONDS)
      logInfo(String.format("Subscribed: %s", subscription))
      subscriptionOpt = Some(subscription)
    } catch {
      case e: ExecutionException =>
        logError("Execution error", e)
        throw e.getCause
      case e: TimeoutException =>
        logError("Timed out subscribing", e)
        throw e.getCause
    }
  }

  def onStop() {
    logDebug("Stopping Salesforce Spark receiver")
    for (subscription <- subscriptionOpt) {
      try {
        val topicSubscription = subscription
        topicSubscription.cancel()
      } catch {
        case e: Exception =>
          logError("SalesForce Spark receiver failed to stop in time", e)
      } finally {
        logDebug("Salesforce Spark receiver stopped")
      }
    }
  }
}
