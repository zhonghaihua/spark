package org.apache.spark.streaming.kafka.jdq.decoder

import com.jd.bdp.jdw.JdwDataSerializer
import com.jd.bdp.jdw.avro.JdwData
import kafka.serializer.Decoder

/**
 * Created by haihua on 16-4-15.
 *
 * The default value decoder
 */
private[streaming]
class JDQByteToJdwDataDecoder
  extends AnyRef with Decoder[JdwData]{

  private val jdwDataDecoder: JdwDataSerializer = new JdwDataSerializer()

  override def fromBytes(bytes: Array[Byte]): JdwData = {
    val messageValue: JdwData = jdwDataDecoder.fromBytes(bytes)
    messageValue
  }
}
