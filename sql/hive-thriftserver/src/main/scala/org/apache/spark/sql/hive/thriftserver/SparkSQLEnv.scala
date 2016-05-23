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

package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream

import com.sun.org.apache.xml.internal.security.utils.Base64

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.hive.{HiveSessionState, HiveUtils}
import org.apache.spark.util.Utils

/** A singleton object for the master program. The slaves should not access this. */
private[hive] object SparkSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  def init() {
    if (sqlContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      //yarn applicationName format of jd-spark
      //begin
      val BUFFALO = "BUFFALO"
      val GATEONE = "GATEONE"
      val CLI = "CLI"
      val IDE = "IDE"
      def decrypt(data: String, key: String): String = {
        val k = (new String(Base64.decode(key))).toInt
        val bData: Array[Byte] = data.getBytes()
        var n = data.getBytes().length
        if ((n%2)!= 0) return ""
        n = n/2
        val buff: Array[Byte] = new Array[Byte](n)
        for (i <- 0 until buff.length) {
          buff(i) = ((n>>(i<<3))&0xFF).toByte
        }
        var j = 0
        for (i <- 0 until n) {
          var data1: Byte = bData(j)
          var data2: Byte = bData(j+1)
          j += 2
          data1 = (data1-65).toByte
          data2 = (data2-65).toByte
          val b2 = (data2*16+data1).toByte
          buff(i) = (b2^k).toByte
        }
        return new String(buff)
      }
      val key = System.getenv("BEE_PASS")
      var schedulerId = System.getenv("BEE_BUSINESSID")
      var sn = System.getenv("BEE_SN")
      var erp = System.getenv("BEE_USER")
      var source = System.getenv("BEE_SOURCE")
      var username = System.getProperty("user.name")
      var ip = Utils.localHostName()
      if (key == null || key.isEmpty
        || BUFFALO.equals(source)
        || BUFFALO.toLowerCase.equals(source)
        || CLI.equals(source)
        || CLI.toLowerCase.equals(source)
        ||((source != null && !source.isEmpty)
        && source.contains(IDE) ||source.contains(IDE.toLowerCase))) {
        if (erp != null && !erp.isEmpty) {
          if (erp.contains("|")) {
            username = erp.substring(0, erp.indexOf("|"))
          }
        }
        if (schedulerId == null || schedulerId.isEmpty) {
          schedulerId = "no"
        }
        if (sn == null || sn.isEmpty) {
          sn = "no"
        }
        if (source == null || source.isEmpty) {
          source = "no"
        }
      }
      else {
        if (erp != null && !erp.isEmpty) {
          username = decrypt(erp, key)
          if (username.contains("|")) {
            username = username.substring(0, username.indexOf("|"))
          }
        }
        if (schedulerId != null && !schedulerId.isEmpty) {
          schedulerId = decrypt(schedulerId, key)
        } else{
          schedulerId = "no"
        }
        if (sn != null && !sn.isEmpty) {
          sn = decrypt(sn, key)
        } else {
          sn = "no"
        }
        if (source != null && !source.isEmpty) {
          source = decrypt(source, key)
        } else {
          source = "no"
        }
      }
      val sessionId = SessionState.get().getSessionId()
      ip = if (ip.isEmpty) "no" else ip

      sparkConf
        .setAppName(s"$username,$ip,$source,$schedulerId,$sn,$sessionId")
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.wrapped

      val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
      sessionState.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      sessionState.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      sessionState.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
      sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (SparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      sqlContext = null
    }
  }
}
