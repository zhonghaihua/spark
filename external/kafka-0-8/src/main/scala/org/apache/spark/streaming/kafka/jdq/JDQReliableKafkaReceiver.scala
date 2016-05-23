package org.apache.spark.streaming.kafka.jdq

import java.util.Properties
import java.util.concurrent.{ThreadPoolExecutor, ConcurrentHashMap}

import com.jd.bdp.jdq.auth.Authentication
import com.jd.bdp.jdq.config.OffsetReset
import com.jd.bdp.jdq.consumer.simple.{JDQSimpleMessage, OutOfRangeHandler, Partition}
import com.jd.bdp.jdq.consumer.zk.MessageHandler
import com.jd.bdp.jdq.consumer.{JDQManualConsumerClient, JDQSimpleConsumer, JDQConsumerClient}
import com.jd.bdp.jdq.offset.OffsetManager
import ex.{JDQException, JDQOverSpeedException, JDQSiteNotFoundException}
import kafka.common.TopicAndPartition
import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils
import org.apache.spark.SparkEnv
import org.apache.spark.storage.{StreamBlockId, StorageLevel}
import org.apache.spark.streaming.receiver.{BlockGeneratorListener, BlockGenerator, Receiver}

import scala.collection.mutable.HashMap
import scala.collection.{mutable, Map}
import scala.reflect._

/**
 * Created by haihua on 16-4-12.
 */
private[streaming]
class JDQReliableKafkaReceiver [
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    offsetCommitMode: Boolean,
    storageLevel: StorageLevel)
    extends Receiver[(K, V)](storageLevel) with Logging {

  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"
  private def conf = SparkEnv.get.conf

  // Connection to jdq
  var consumer: JDQSimpleConsumer = null
  @transient var auth: Authentication = null
  @transient var offsetManager: OffsetManager = null

  /**
   * A HashMap to manage the offset for each topic/partition, this HashMap is called in
   * synchronized block, so mutable HashMap will not meet concurrency issue.
   */
  private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

  /** A concurrent HashMap to store the stream block id and related offset snapshot. */
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

  /**
   * Manage the BlockGenerator in receiver itself for better managing block store and offset
   * commit.
   */
  private var blockGenerator: BlockGenerator = null

  override def onStart(): Unit = synchronized {
    logInfo("Starting jdq Consumer Stream with app: " + kafkaParams("appId"))

    val stopConsume: Boolean = false
    var speed: Int = 0
    var isSpeedDown: Boolean = false

    // Initialize the topic-partition / offset hash map.
    topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

    // Initialize the stream block id / offset snapshot hash map.
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]()

    // Initialize the block generator for storing Kafka message.
    blockGenerator = new BlockGenerator(new GeneratedBlockHandler, streamId, conf)

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in JDQReliableKafkaReceiver, " +
        "otherwise we will manually set it to false to turn off auto offset commit in jdq")
    }

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    props.setProperty(AUTO_OFFSET_COMMIT, "false")

    val token = kafkaParams("token")
    val appId = kafkaParams("appId")
//    val commitOffSet = kafkaParams("commitOffSet")
    //    val consumerConfig = new ConsumerConfig(props)
    auth = new Authentication(appId, token)
    offsetManager = OffsetManager.getInstance()
    consumer = new JDQSimpleConsumer(auth)
    speed = topics.values.sum
    val partitionOffsetMap: scala.collection.mutable.HashMap[Partition, Long] = new HashMap[Partition, Long]()

    val kDecoder = classTag[U].runtimeClass.newInstance().asInstanceOf[Decoder[K]]
    val vDecoder = classTag[T].runtimeClass.newInstance().asInstanceOf[Decoder[V]]
    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "JDQKafkaReliableMessageHandler")

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

    blockGenerator.start()

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
      consumer.close()
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }

  }

  override def onStop(): Unit = {

    if (consumer != null) {
      consumer.close()
      consumer = null
    }

    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }

    if (topicPartitionOffsetMap != null) {
      topicPartitionOffsetMap.clear()
      topicPartitionOffsetMap = null
    }

    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  /** Store a jdq message and the associated metadata as a tuple. */
  private def storeMessageAndMetadata(key: K, value: V, offSet: Long, partition: Int): Unit = {
    val topicAndPartition = TopicAndPartition("current", partition)
    val data = (key, value)
    val metadata = (topicAndPartition, offSet)
    blockGenerator.addDataWithCallback(data, metadata)
  }

  /** Update stored offset */
  private def updateOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    topicPartitionOffsetMap.put(topicAndPartition, offset)
  }

  /**
   * Remember the current offsets for each topic and partition. This is called when a block is
   * generated.
   */
  private def rememberBlockOffsets(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current offset map and store with related block id.
    val offsetSnapshot = topicPartitionOffsetMap.toMap
    blockOffsetMap.put(blockId, offsetSnapshot)
    topicPartitionOffsetMap.clear()
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to zookeeper. This method
   * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
   */
  private def storeBlockAndCommitOffset(
      blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      Option(blockOffsetMap.get(blockId)).foreach(commitOffset)
      blockOffsetMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * Commit the offset of jdq's partition.
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (consumer == null) {
      val thrown = new IllegalStateException("jdq simple consumer is unexpectedly null")
      stop("jdq simple consumer is not initialized before commit offsets to jdq", thrown)
      return
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val partition = topicAndPart.partition
        if (offsetCommitMode) {
          offsetManager.commitOffsetAsync(auth, partition, offset)
        } else {
          offsetManager.commitOffset(auth, partition, offset)
        }
      } catch {
        case e: Exception =>
          logWarning(s"Exception during commit offset $offset for topic" +
            s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e)
      }

      logInfo(s"Committed offset $offset for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }

//  private class JDQMessageHandler
//    extends MessageHandler[K, V] {
//    def doMessageHandler(key: K, value: V, offSet: Long, partition: Int) {
//      logInfo("Starting MessageHandler.  key: " + key.toString + " value: " + value.toString)
//      storeMessageAndMetadata(key, value, offSet, partition)
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
        storeMessageAndMetadata(key, value, message.getNextOffset, message.getPartition)
      }
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      if (metadata != null) {
        val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)]
        updateOffset(topicAndPartition, offset)
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the offsets of topics/partitions when a block has been generated
      rememberBlockOffsets(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

  private class JDQOutOfRangeHandler extends OutOfRangeHandler {
    override def handleOffsetOutOfRange (earliestOffset: Long, latestOffset: Long, currentOffset: Long): Long = {
      latestOffset
    }
  }
}
