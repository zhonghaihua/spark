package org.apache.spark.streaming.kafka.jdq.decoder

import com.jd.bdp.jdw.JdwDataSerializer
import com.jd.bdp.jdw.avro.JdwData
import kafka.serializer.Encoder

/**
 * Created by haihua on 16-4-15.
 */
//private[streaming]
class JDQJdwDataToByteEncoder
  extends AnyRef with Encoder[JdwData] {

  private val jdwDataDecoder: JdwDataSerializer = new JdwDataSerializer()

  override def toBytes(jdwData: JdwData): Array[Byte] = {
    val bytes: Array[Byte] = jdwDataDecoder.toBytes(jdwData)
    bytes
  }

}
