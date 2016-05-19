package org.apache.spark.streaming.kafka.jdq.decoder

import kafka.serializer.Decoder

import java.io.UnsupportedEncodingException
/**
 * Created by haihua on 16-4-15.
 *
 * The default key decoder
 */
//private[streaming]
class JDQStringDecoder
  extends AnyRef with Decoder[String] {

  override def fromBytes(bytes: Array[Byte]): String = {
    val value = new String(bytes, "utf-8")
    value
  }
}
