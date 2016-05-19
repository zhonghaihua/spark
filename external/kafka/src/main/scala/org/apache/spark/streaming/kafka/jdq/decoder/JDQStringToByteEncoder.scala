package org.apache.spark.streaming.kafka.jdq.decoder

import kafka.serializer.Encoder

/**
 * Created by haihua on 16-4-15.
 */
private[streaming]
class JDQStringToByteEncoder
  extends AnyRef with Encoder[String] {

  override def toBytes(msg: String): Array[Byte] = {
    val value: Array[Byte] = msg.getBytes
    value
  }
}
