/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}

import org.apache.spark.{Logging, SparkConf, SecurityManager}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.util.AkkaUtils
import org.apache.spark.streaming.receiver.ActorHelper

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2
 *  second delay.
 */
class FeederActor extends Actor {

  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(500)
        receivers.foreach(_ ! makeMessage)
      }
    }
  }.start()

  def receive: Receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
    receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
    receivers = receivers.dropWhile(x => x eq receiverActor)

  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 * @see [[org.apache.spark.examples.streaming.FeederActor]]
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String)
extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
object FeederActor {

  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("Usage: FeederActor <hostname> <port>\n")
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq

    val conf = new SparkConf
    val actorSystem = AkkaUtils.createActorSystem("test", host, port.toInt, conf = conf,
      securityManager = new SecurityManager(conf))._1
    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: ActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/run-example org.apache.spark.examples.streaming.FeederActor 127.0.1.1 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.ActorWordCount 127.0.1.1 9999`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /*
     * Following is the use of actorStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream
     * should be same.
     *
     * For example: Both actorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */

    val lines = ssc.actorStream[String](
      Props(new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(
        host, port.toInt))), "SampleReceiver")

    // compute wordcount
    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println

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
