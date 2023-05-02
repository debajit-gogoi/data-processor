package com.debajit.dataProcessor.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * @author Debajit
 */

object KafkaHelper extends LazyLogging {

  def kafkaConsumer: (String, String) => KafkaConsumer[Array[Byte], Array[Byte]] =
    (groupId, bootstrap_servers) => {
      val props = new Properties()
      props put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
      props put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      props put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      props put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      props put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      new KafkaConsumer[Array[Byte], Array[Byte]](props)
    }

  def getStartingOffsetsString(kafkaConsumer: KafkaConsumer[_, _], topic: String): String = {
    Try {
      import scala.collection.JavaConverters._
      val partitions: List[TopicPartition] = getPartitions(kafkaConsumer, topic)
      kafkaConsumer assign partitions.asJava
      val startOffsets: Map[String, Map[String, Long]] = getPartitionOffsets(kafkaConsumer, topic, partitions)
      println(s"Starting offsets for $topic: ${startOffsets(topic).filterNot(_._2 == -2L)}")
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      Serialization write startOffsets
    } match {
      case Success(jsonOffsets) => jsonOffsets
      case Failure(e) =>
        println(s"Failed to retrieve starting offsets for $topic: ${e.getMessage}")
        "earliest"
    }
  }

  def getPartitions(kafkaConsumer: KafkaConsumer[_, _], topic: String): List[TopicPartition] = {
    import scala.collection.JavaConverters._
    (kafkaConsumer.partitionsFor(topic)
      .asScala map (p => new TopicPartition(topic, p partition())))
      .toList
  }

  def getPartitionOffsets(kafkaConsumer: KafkaConsumer[_, _], topic: String, partitions: List[TopicPartition]): Map[String, Map[String, Long]] = {
    Map(
      topic -> partitions.map(p => p.partition().toString -> kafkaConsumer.position(p)).map({
              case (partition, offset) if offset == 0L => partition -> -2L
              case mapping => mapping
            })
        .toMap
    )
  }

  def commitKafkaOffset(kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]], topic: String, commit: String): Unit = {
    Try {
      import scala.collection.JavaConverters._
      val partitions: List[TopicPartition] = KafkaHelper getPartitions(kafkaConsumer, topic)
      kafkaConsumer seekToEnd partitions.asJava
      val endOffsets: Map[String, Map[String, Long]] = KafkaHelper getPartitionOffsets(kafkaConsumer, topic, partitions)
      println(s"Ending offsets for $topic: ${endOffsets(topic).filterNot(_._2 == -2L)}")
      commit match {
        case "true" => println(s"Committing offset for topic $topic")
          kafkaConsumer commitSync()
        case "false" => println(s"Commit offset is set to false for topic $topic")
      }
      kafkaConsumer close()
    } match {
      case Success(_) => ()
      case Failure(e) =>
        println(s"Failed to set offsets for $topic: ${e.getMessage}")
    }
  }
}
