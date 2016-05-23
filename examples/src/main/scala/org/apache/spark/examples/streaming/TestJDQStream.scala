package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * Created by haihua on 16-5-23.
 */
object TestJDQStream extends Logging {
  def main (args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: TestJDQStream <appId> <token>")
      System.exit(1)
    }
    val result: Array[String] = new Array[String](100)
    var count = 0
    val Seq(appId, token) = args.toSeq
    val sparkConf = new SparkConf().setAppName("TestJDQStream")
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

    val kafkaParams = Map("appId" -> appId, "token" -> token, "offSet" -> "now", "commitOffSet" -> "5")
    val topic = Map("jdqtest" -> 8)

    val stream = KafkaUtils.createJDQStream(ssc, kafkaParams, topic, StorageLevel.MEMORY_AND_DISK_SER_2)

    //    stream.map(str => {
    //      if (count < 100) {
    //        result(count) = str._1
    //        count += 1
    //      }
    //      (str._1, str._2)
    //    }).print()
    //
    //    logInfo("-------------array: " + result(12))
    //
    //    stream.foreachRDD ((rdd: RDD[(String, String)]) => {
    //      rdd.map(msg => {
    //        logInfo("----------msg:" + msg.toString)
    //        //        val byteData = new JDQJdwDataToByteEncoder().toBytes(msg._2)
    //        //        val stringData = new JDQStringDecoder().fromBytes(byteData)
    //        logInfo("-----------key: " + msg._1 + " value: " + msg._2)
    //        (msg._1, msg._2)
    //      })
    //    })

    val keyValue = stream.map(str => {
      val value = str._2.split("\t")
      val pstr = value(2).concat("ipadress")
      //      logInfo(pstr)
      (value(2), 1)
    })

    val counts2 = keyValue.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var c: Int = state.getOrElse(0)
      for (i: Int <- values) {
        c += i
      }
      Option(c)
    })

    counts2.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
