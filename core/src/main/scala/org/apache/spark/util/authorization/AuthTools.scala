package org.apache.spark.util.authorization

import java.io.{IOException, OutputStreamWriter, PrintWriter}
import java.lang.management.ManagementFactory
import java.net.{HttpURLConnection, InetAddress, URL, URLEncoder}
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import jline.console.ConsoleReader
import org.apache.hadoop.hive.ql.metadata.{AuthorizationException, HiveException}
import org.apache.hadoop.hive.ql.security.authorization.Privilege
import org.apache.spark.util.Base64
import org.apache.spark.util.authorization.AuthType.AuthType
import org.apache.spark.{Logging, SparkConf}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.io.File
import scala.sys.process._

/**
 * Created by wubiao on 5/9/16.
 */
object AuthTools extends Logging {
  val conf = new SparkConf()
  val SPARK_URM_AUTHORIZATION_ENABLED = "spark.urm.authorization.enabled"
  val SPARK_URM_HTTP_ADDRESS = "spark.urm.http-address"
  val SPARK_ERP_AUTHORIZATION_HTTP_ADDRESS = "spark.erp.authorization.http-address"
  val SPARK_CLUSTER_ID = "spark.cluster.id"
  val SPARK_URM_TIMEOUT = "spark.urm.timeout"
  val SPARK_URM_RETRY = "spark.urm.retry"
  val SPARK_URM_AUTHORIZATION_MANAGER = "spark.urm.authorization.manager"
  val sparkUrmAuthEnabled = conf.getBoolean(SPARK_URM_AUTHORIZATION_ENABLED, false)
  val sparCheckErp = if (sparkUrmAuthEnabled) new URL(conf.get(SPARK_ERP_AUTHORIZATION_HTTP_ADDRESS, "http://bdp.jd.com/api/urm/erp/check.ajax")) else null
  val sparkUrmHost: URL = if (sparkUrmAuthEnabled) new URL(conf.get(SPARK_URM_HTTP_ADDRESS, "http://urm2.bdp.jd.local")) else null
  val sparkUrmAuthorizationManager = conf.get(SPARK_URM_AUTHORIZATION_MANAGER, "org.apache.spark.sql.hive.HiveURMAuthorizationProvider")

  // URM Operation API
  val urmCheckRight = getApi("/api/urm/jdh/hive/check-right.ajax")
  val urmShowDb = getApi("/api/urm/jdh/show/database.ajax")
  val urmShowTB = getApi("/api/urm/jdh/show/table.ajax")
  val urmCreate = getApi("/api/urm/jdh/ddl/create.ajax")
  val urmAlter = getApi("/api/urm/jdh/ddl/alter.ajax")
  val urmDrop = getApi("/api/urm/jdh/ddl/drop.ajax")
  val urmTimeOut = conf.getInt(SPARK_URM_TIMEOUT, 3000)
  val urmRetry = conf.getInt(SPARK_URM_RETRY, 3)
  val clusterId = conf.get(SPARK_CLUSTER_ID, "")


  // ENV
  val env: Map[String, String] = sys.env
  val BEE_SOURCE = "BEE_SOURCE"
  val BEE_USER = "BEE_USER"
  val BEE_PASS = "BEE_PASS"
  val BUFFALO = "BUFFALO"
  val IDE = "IDE"
  val GATEONE = "GATEONE"
  val CLI = "CLI"
  val CURRENT_USER: String = System.getProperty("user.name")
  val TMP_DIR: String = System.getProperty("java.io.tmpdir")

  // static var
  var erp: String = getErp()

  def getApi(op: String): URL = {
    if (sparkUrmAuthEnabled) {
      new URL(sparkUrmHost, op)
    } else {
      null
    }
  }

  def isIDE(): Boolean = getSource().startsWith(IDE)

  def isBuffalo(): Boolean = getSource() == BUFFALO

  def isGateOne(): Boolean = getSource() == GATEONE

  /**
   * Get spark env
   * @return [gateone,buffalo,cli,ide]
   */
  def getSource(): String = {
    var source: String = getEnv(BEE_SOURCE)
    if (source == null) {
      source = CLI
    } else {
      try {
        source = decrypt(source, getEnv(BEE_PASS))
        if (source == null || "".equals(source.trim)) {
          source = getEnv(BEE_SOURCE)
        }
      } catch {
        case e: Exception => source = getEnv(BEE_SOURCE)
      }
    }
    source.toUpperCase
  }

  def isNeedCheck(): Boolean = {
    getSource() == CLI
  }

  def getEnv(key: String) = {
    Option(env.get(key)).filterNot(_.isEmpty).map(_.toString).orElse(conf.getOption(key)).orNull
  }

  /**
   * decrypt code from gateone
   * @param data
   * @param key
   * @return
   */
  def decrypt(data: String, key: String): String = {
    val k = (new String(Base64.decode(key))).toInt
    val bData: Array[Byte] = data.getBytes()
    var n = data.getBytes().length
    if ((n % 2) != 0) return ""
    n = n / 2
    val buff: Array[Byte] = new Array[Byte](n)
    for (i <- 0 until buff.length) {
      buff(i) = ((n >> (i << 3)) & 0xFF).toByte
    }
    var j = 0
    for (i <- 0 until n) {
      var data1: Byte = bData(j)
      var data2: Byte = bData(j + 1)
      j += 2
      data1 = (data1 - 65).toByte
      data2 = (data2 - 65).toByte
      val b2 = (data2 * 16 + data1).toByte
      buff(i) = (b2 ^ k).toByte
    }
    return new String(buff)
  }

  def exitSpark(exception: Exception): Unit = {
    exception.printStackTrace()
    System.exit(-1)
  }

  def printWarning(message: String): Unit = {
    try {
      throw new Exception(message)
    } catch {
      case e: Exception =>
        if (sparkUrmAuthEnabled) exitSpark(e)
    }
  }

  def getIp(): String = {
    val ip = InetAddress.getLocalHost.getHostAddress
    if (ip != null && ip != "") {
      ip
    } else {
      try {
        val rt = Runtime.getRuntime
        val proc = rt.exec(Array("/bin/sh", "-c", "hostname -i"), null, null)
        val ip = Source.fromInputStream(proc.getInputStream).getLines().next()
        if (ip != null && ip != "") {
          ip
        } else {
          ""
        }
      } catch {
        case e: Exception =>
          logError("get ip error!!")
          ""
      }
    }
  }

  def getLinuxSessionId(): String = {
    val pid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
    ("ps -o pid,sid " #| "grep -v grep " #| Seq("awk", s"""{if($$1==$pid) print $$2}""")).!!.trim
  }

  def getLoginTime(): Long = {
    0
  }

  /**
   * get user's erp info
   * @return
   */
  def getErp(): String = {
    val erpFromEnv = getEnv(BEE_USER)
    val pass = getEnv(BEE_PASS)
    var erp: String = null
    if (this.erp != null) {
      return this.erp
    }
    if (pass == null) {
      erp = erpFromEnv
    } else {
      try {
        val dErp = decrypt(erpFromEnv, pass)
        if (getSource() == GATEONE) {
          if (dErp == null) {
            printWarning(s"decrypt length is error! data:null,the environment has been changed to [User:${erpFromEnv}]")
          }
          val decryptData = dErp.split("\\|")
          if (decryptData.length != 4) {
            printWarning(s"decrypt length is error! data:${decryptData},the environment has been changed to [User:${erpFromEnv}]")
          }
          if (decryptData(3) != getIp()) {
            printWarning(s"IP is illegal! Local:${getIp()},Request:${decryptData(3)}")
          }
          erp = decryptData(0)
        }
      } catch {
        case e: Exception =>
          printWarning(s"decrypt error! USER:${erpFromEnv},Key:${pass}")
      }
    }
    this.erp = erp
    erp
  }

  /**
   * user login
   * @param erp
   * @param password
   * @return
   */
  def authErp(erp: String, password: String): Boolean = {
    val stringBuilder = new StringBuilder("")
    try {
      val connection = sparCheckErp.openConnection()
      connection.setConnectTimeout(5 * 1000); //设置超时时间
      connection.setDoOutput(true)
      connection.setDoInput(true)
      val out = new OutputStreamWriter(connection.getOutputStream(), "UTF-8")
      out.write("erp=" + URLEncoder.encode(erp, "UTF-8") + "&pwd=" + URLEncoder.encode(password, "UTF-8")) // 向页面传递数据。post的关键所在！
      out.flush()
      out.close()
      val inputStream = connection.getInputStream
      Source.fromInputStream(inputStream).getLines().foreach(stringBuilder.append(_))
      inputStream.close()
      val pattern = Pattern.compile("(.*)(\"code\":[0-9]+)(.*)")
      val m = pattern.matcher(stringBuilder.toString.replaceAll(" ", ""))
      if (m.matches()) {
        "0".equals(m.group(2).split(":")(1).replaceAll("\"", ""))
      } else {
        false
      }
    } catch {
      case e: Exception =>
        logError(e.getMessage())
        logError("User authentication fails, the default authentication")
        true // 出错时，默认正确
    }
  }

  /**
   * user login by erp&password or session info
   * @return
   */
  def loginAuth(): Boolean = {
    if (!sparkUrmAuthEnabled) {
      true
    } else if (!isNeedCheck()) {
      true
    } else {
      val sessionId = getLinuxSessionId()
      // get sessionId md5 code
      val rt = Runtime.getRuntime
      val proc = rt.exec(Array("/bin/sh", "-c", s"echo  ${sessionId} | md5sum"), null, null)
      val sessionIdMd5 = Source.fromInputStream(proc.getInputStream).getLines().next()
      val sessionDir = File(TMP_DIR + File.separator + CURRENT_USER)
      if (sessionDir.notExists) sessionDir.createDirectory()
      val sessionFile = File(sessionDir + File.separator + "." + sessionIdMd5)
      if (sessionFile.exists) {
        val lastModified = sessionFile.lastModified
        if (getLoginTime() > lastModified) login(sessionFile)
        else {
          this.erp = Source.fromFile(sessionFile.jfile).getLines().next()
          true
        }
      } else {
        login(sessionFile)
      }
    }
  }

  def login(sessionFile: File): Boolean = {
    println("######################## JDH LOGIN ###########################\n"
      + "Please enter your ERP username and password : ")
    val mask = new Character('*');
    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    reader.setExpandEvents(false)
    var user = reader.readLine("Entry UserName: ")
    while (user == null || "" == user.trim) {
      user = reader.readLine("Entry UserName: ")
    }
    var password = reader.readLine("Entry Password: ", mask)
    while (password == null || "" == password.trim) {
      password = reader.readLine("Entry Password: ", mask)
    }
    val result = authErp(user, password)
    if (result) {
      this.erp = user
      val writer = new PrintWriter(sessionFile.jfile)
      writer.write(user)
      writer.close()
    }
    result
  }

  def authorize(location: String, parentLocation: String, privileges: Array[Privilege], dbName: String, tableName: String): Unit = {
    if (privileges == null || privileges.length == 0) {
      return
    }
    val loc = if (location == null) clusterId + ":" + System.getProperty("user.name") else clusterId + ":" + location
    val pLoc = if (parentLocation == null) "" else clusterId + ":" + parentLocation
    val privilegeArray = privileges.map(_.toString.toUpperCase())
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("location", loc)
    sendData.put("action", privilegeArray)
    sendData.put("cluster", clusterId)
    if (dbName == null && tableName == null) {
      logInfo("HDFS path return")
      return
    } else {
      logInfo("Table/Database permissions authentication")
      sendData.put("parent_location", pLoc)
    }
    logInfo(s"Request URM:$sendData,URL:$urmCheckRight")
    try {
      val result: String = httpPostJsonAsParam(urmCheckRight, sendData.toJSONString, urmTimeOut, urmRetry)
      logInfo(s"URM response result: $result")
      val resultJson = JSON.parseObject(result)
      if (!resultJson.getBoolean("success")) {
        checkAndThrowAuthorizationException(
          JSON.parseArray(resultJson.getString("list"), classOf[String]).asInstanceOf[List[String]],
          dbName, tableName, resultJson.getString("error"))
      }
    } catch {
      case e: AuthorizationException =>
        throw e
      case e: Exception =>
        if (privilegeArray.contains(Privilege.CREATE.getPriv.name()) ||
          privilegeArray.contains(Privilege.ALTER_METADATA.getPriv.name()) ||
          privilegeArray.contains(Privilege.DROP.getPriv.name())) {
          checkAndThrowAuthorizationException(privilegeArray.toList, dbName, tableName, e.getMessage)
        } else {
          logInfo(s"Request URM fail,the default authentication success ->request URM: $sendData")
        }
    }

  }

  def showTablesByPattern(location: String, pattern: String = ""): Seq[String] = {
    val loc = s"$clusterId:$location"
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("location", loc)
    sendData.put("pattern", pattern.replaceAll("\\*", ".*"))
    sendData.put("cluster", clusterId)
    logInfo(s"Request URM get Table list:$sendData,URL:$urmShowTB")
    val result = httpPostJsonAsParam(urmShowTB, sendData.toJSONString)
    logInfo(s"URM response result:$result")
    val urmResponse = JSON.parseObject(result)
    if (urmResponse.getString("success") == "false") {
      throw new HiveException(urmResponse.getString("error"))
    } else {
      JSON.parseArray(urmResponse.getString("list"), classOf[String]).asScala
    }
  }

  def showDatabasesByPattern(pattern: String = ""): Seq[String] = {
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("key", System.getProperty("user.name"))
    sendData.put("pattern", pattern.replaceAll("\\*", ".*"))
    sendData.put("cluster", clusterId)
    logInfo(s"Request URM get Database list:$sendData,URL:$urmShowDb")
    val result = httpPostJsonAsParam(urmShowDb, sendData.toJSONString)
    logInfo(s"URM response result:$result")
    val urmResponse = JSON.parseObject(result)
    if (urmResponse.getString("success") == "false") {
      throw new HiveException(urmResponse.getString("error"))
    } else {
      JSON.parseArray(urmResponse.getString("list"), classOf[String]).asScala
    }
  }

  def httpPostJsonAsParam(url: URL, data: String, timeout: Int = urmTimeOut, retryTime: Int = urmRetry): String = {
    val jsonData = JSON.parseObject(data, classOf[java.util.Map[String, Object]]).asScala
    for (i <- 1 to retryTime) {
      val paramBuilder = new StringBuilder
      jsonData.foreach(e => {
        val (k, v) = e
        paramBuilder.append(s"&$k=")
        if (v.isInstanceOf[JSONArray]) {
          paramBuilder.append(URLEncoder.encode(v.asInstanceOf[JSONArray].asScala.mkString(","), "UTF-8"))
        } else {
          paramBuilder.append(URLEncoder.encode(v.toString, "UTF-8"))
        }
      })
      try {
        return httpPost(url, paramBuilder.drop(1).toString(), timeout)
      } catch {
        case e: Exception =>
          if (retryTime == i) {
            logError(s"Request URM[${url.toString}] fail!")
            throw e
          } else {
            logError(s"Request URM[${url.toString}] fail,begin to retry :${i + 1}/$retryTime")
          }
      }
    }
    ""
  }

  def httpPost(url: URL, data: String, timeout: Int): String = {
    val start = System.currentTimeMillis()
    val stringBuilder = new StringBuilder("")
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(timeout)
    connection.setReadTimeout(timeout)
    connection.setDoOutput(true)
    connection.setDoInput(true)
    val out = new OutputStreamWriter(connection.getOutputStream(), "UTF-8")
    out.write(data)
    out.flush()
    out.close()
    val inputStream = connection.getInputStream
    val code = connection.getResponseCode
    Source.fromInputStream(inputStream).getLines().foreach(stringBuilder.append(_))
    inputStream.close()
    val end = System.currentTimeMillis()
    logInfo(s"${url.toString} response time: ${end - start} ms")
    if (code != HttpURLConnection.HTTP_OK) {
      throw new IOException(s"HTTP response code:$code")
    }
    stringBuilder.toString()
  }

  /**
   * check authistoryserverh and throw exception
   * @param privilege
   * @param dbName
   * @param tableName
   * @param error
   */
  def checkAndThrowAuthorizationException(privilege: List[String], dbName: String, tableName: String, error: String): Unit = {
    val message = new StringBuilder("")
    if (dbName != null) {
      message.append(s"database:$dbName")
    }
    if (tableName != null) {
      message.append(s",table:$tableName")
    }
    if (error != null && error.trim != "") {
      throw new AuthorizationException(s"$privilege for $message urm response error:$error.")
    }
    if (privilege != null && privilege.isEmpty) {
      throw new AuthorizationException(s"No privilege $privilege found for $message.")
    }
  }

  def createOperationUrm(location: String, parentLocation: String, tp: AuthType, name: String, nameZh: String): Unit = {
    val loc = s"$clusterId:$location"
    val pLoc = s"$clusterId:$parentLocation"
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("location", loc)
    sendData.put("parent_location", pLoc)
    sendData.put("user", System.getProperty("user.name"))
    sendData.put("type", tp.id)
    sendData.put("name", name)
    sendData.put("nameZh", if (nameZh == null || nameZh == "") name else nameZh)
    sendData.put("cluster", clusterId)
    logInfo(s"Request URM DDL create operation:$sendData")
    val result = httpPostJsonAsParam(urmCreate, sendData.toJSONString)
    logInfo(s"Response URM DDL create operation:$result")
    val urmResponse = JSON.parseObject(result)
    if (urmResponse.getString("success") == "false") {
      throw new HiveException(urmResponse.getString("error"))
    }
  }

  def alterOperationUrm(newLocation: String, newName: String): Unit = {
    val newLoc = s"$clusterId:$newLocation"
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("now_l", newLoc)
    sendData.put("now_name", newName)
    sendData.put("cluster", clusterId)
    logInfo(s"Request URM DDL Alter operation:$sendData")
    val result = httpPostJsonAsParam(urmAlter, sendData.toJSONString)
    logInfo(s"Response URM DDL Alter operation:$result")
    val urmResponse = JSON.parseObject(result)
    if (urmResponse.getString("success") == "false") {
      throw new HiveException(urmResponse.getString("error"))
    }
  }

  def dropOperationUrm(location: String, tp: AuthType): Unit = {
    val loc = s"$clusterId:$location"
    val sendData = new JSONObject()
    sendData.put("erp", erp)
    sendData.put("location", loc)
    sendData.put("cluster", clusterId)
    sendData.put("type", tp.id)
    logInfo(s"Request URM DDL Drop operation:$sendData")
    val result = httpPostJsonAsParam(urmDrop, sendData.toJSONString)
    logInfo(s"Response URM DDL Drop operation:$result")
    val urmResponse = JSON.parseObject(result)
    if (urmResponse.getString("success") == "false") {
      throw new HiveException(urmResponse.getString("error"))
    }
  }

}

object AuthType extends Enumeration {
  type AuthType = Value
  val TABLE = Value(0)
  val DATABASE = Value(1)
}