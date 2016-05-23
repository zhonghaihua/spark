package org.apache.spark.streaming.kafka.jdq

import java.util.Properties
import java.util.concurrent.ThreadPoolExecutor

import com.jd.bdp.jdq.auth.Authentication
import com.jd.bdp.jdq.consumer.simple.{Partition, OutOfRangeHandler, JDQSimpleMessage}
import com.jd.bdp.jdq.consumer.{JDQSimpleConsumer, JDQConsumerClient}
import com.jd.bdp.jdq.offset.OffsetManager
import ex.{JDQException, JDQSiteException, JDQSiteNotFoundException, JDQOverSpeedException}
import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils


import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.parallel.mutable
import scala.reflect._

/**
 * Created by haihua on 16-4-12.
 */
private[streaming]
class JDQKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    useReliableReceiver: Boolean,
    offsetCommitMode: Boolean,
    storageLevel: StorageLevel
    ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  def getReceiver(): Receiver[(K, V)] = {
    if (!useReliableReceiver) {
      new JDQKafkaReceiver[K, V, U, T](kafkaParams, topics, offsetCommitMode, storageLevel)
    } else {
      new JDQReliableKafkaReceiver[K, V, U, T](kafkaParams, topics, offsetCommitMode, storageLevel)
    }
  }
}

private[streaming]
class JDQKafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    offsetCommitMode: Boolean,
    storageLevel: StorageLevel
    ) extends Receiver[(K, V)](storageLevel) with Logging {

  // Connection to Kafka
  var consumer: JDQSimpleConsumer = null
  logInfo("------------init jdqkafkaReceiver--------")

  def onStop() {
    if (consumer != null) {
      consumer.close()
      consumer = null
    }
  }

  def onStart(): Unit = synchronized {

    logInfo("Starting Kafka Consumer Stream with app: " + kafkaParams("appId"))
    logInfo("----------topic.values.sum: " + topics.values.sum)

    val stopConsume: Boolean = false
    var speed: Int = 0
    var isSpeedDown: Boolean = false
    val partitionOffsetMap: scala.collection.mutable.HashMap[Partition, Long] = new HashMap[Partition, Long]()

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val token = kafkaParams("token")
    val appId = kafkaParams("appId")
    val offSet = kafkaParams("offSet")
    val commitOffSet = kafkaParams("commitOffSet")
//    val consumerConfig = new ConsumerConfig(props)
    val auth: Authentication = new Authentication(appId, token)
    consumer = new JDQSimpleConsumer(auth)
    speed = topics.values.sum

    val kDecoder = classTag[U].runtimeClass.newInstance().asInstanceOf[Decoder[K]]
    val vDecoder = classTag[T].runtimeClass.newInstance().asInstanceOf[Decoder[V]]
    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "JDQKafkaMessageHandler")

    val offsetManager: OffsetManager = OffsetManager.getInstance()
    val outOfRangeHandler: JDQOutOfRangeHandler = new JDQOutOfRangeHandler()
    val partitions = consumer.findPartitions().toArray().map { e => e.asInstanceOf[Partition] }

    partitions.foreach { partition =>
      var initOffset: Long = 0
      try {
        initOffset = offsetManager.findOffset(auth, partition.getPartition)
      } catch {
        case notFound: JDQSiteNotFoundException =>
          initOffset = consumer.findValidityMinOffset(partition.getPartition)
      }
      partitionOffsetMap(partition) = initOffset
    }

    var timeCount = 1
    try {
      while (!stopConsume) {
        logInfo("--------------- timeCount: " + timeCount)
        timeCount += 1
        while (executorPool.getActiveCount >= speed) {}
        partitions.foreach { partition =>
          logInfo("-----partition: " + partition.getPartition + " minOffset: " +
            consumer.findValidityMinOffset(partition.getPartition) + " maxOffset: " +
            consumer.findValidityMaxOffset(partition.getPartition))
          val offset = partitionOffsetMap(partition)
          var messages: Array[JDQSimpleMessage] = null
          try {
            messages = consumer.consumeMessage(partition.getPartition, offset, outOfRangeHandler)
              .toArray().map { e => e.asInstanceOf[JDQSimpleMessage] }
            if (messages != null && messages.size > 0) {
              val commitMaxOffset = messages.map(msg => (msg.getNextOffset)).max
              partitionOffsetMap(partition) = commitMaxOffset
              if (offsetCommitMode) {
                offsetManager.commitOffsetAsync(auth, partition.getPartition, commitMaxOffset)
              } else {
                offsetManager.commitOffset(auth, partition.getPartition, commitMaxOffset)
              }
              executorPool.submit(new JDQMessageHandler(messages, kDecoder, vDecoder))
              if (isSpeedDown) {
                speed -= 1
                isSpeedDown = false
              }
            }
          } catch {
            case e: JDQOverSpeedException =>
              isSpeedDown = true
              logError("consume overspeed: " + e.getMessage)
          }
        }
        if (speed <= 1) {
          speed = 1
        }
      }
    } catch {
      case e: JDQException =>
        logError("jdq Exeception: " + e.getMessage)
        throw e
    } finally {
      partitions.foreach { partition =>
        offsetManager.commitOffset(auth, partition.getPartition, partitionOffsetMap(partition))
      }
      consumer.close()
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

//  private class JDQMessageHandler
//    extends MessageHandler[K, V] {
//    def doMessageHandler(key: K, value: V, offSet: Long, partition: Int) {
//      logInfo("Starting MessageHandler.  key: " + key.toString + " value: " + value.toString)
//      store((key, value))
//      logInfo("key: " + key.toString + " value: " + value.toString)
//    }
//  }
  private class JDQMessageHandler(messages: Array[JDQSimpleMessage], kDecoder: Decoder[K], vDecoder: Decoder[V])
    extends Runnable {
    def run() {
      messages.foreach { message =>
        logInfo("Starting MessageHandler.  offset: " + message.getOffset() + " partition: " +
          message.getPartition() + " key: " + kDecoder.fromBytes(message.getKey()) + " value: " +
          vDecoder.fromBytes(message.getData()))
        val key: K = kDecoder.fromBytes(message.getKey())
        val value: V = vDecoder.fromBytes(message.getData())
        store((key, value))
      }
    }
  }

  private class JDQOutOfRangeHandler
    extends OutOfRangeHandler {
    override def handleOffsetOutOfRange(earliestOffset: Long, latestOffset: Long, currentOffset: Long): Long = {
      latestOffset
    }
  }
}
