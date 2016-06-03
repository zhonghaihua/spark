package org.apache.spark.sql.hive

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.ql.hooks.{Entity, ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.hadoop.hive.ql.security.authorization.{HiveAuthorizationProvider, HiveAuthorizationProviderBase, Privilege}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.util.authorization.AuthTools

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet

/**
 * URM AuthorizationProvider
 * Created by wubiao on 5/17/16.
 */
class HiveURMAuthorizationProvider extends HiveAuthorizationProviderBase {

  override def authorize(inputRequiredPriv: Array[Privilege], outputRequiredPriv: Array[Privilege]): Unit = {

  }

  override def authorize(database: Database, inputRequiredPriv: Array[Privilege], outputRequiredPriv: Array[Privilege]): Unit = {
    AuthTools.authorize(database.getLocationUri, null,
      getCollectArray(inputRequiredPriv, outputRequiredPriv),
      database.getName, null)
  }

  override def authorize(table: Table, inputRequiredPriv: Array[Privilege], outputRequiredPriv: Array[Privilege]): Unit = {
    AuthTools.authorize(if (table.getDataLocation == null) "" else table.getDataLocation.toString,
      hive_db.getDatabase(table.getDbName).getLocationUri,
      getCollectArray(inputRequiredPriv, outputRequiredPriv), table.getDbName, table.getTableName)
  }

  override def authorize(partition: Partition, inputRequiredPriv: Array[Privilege], outputRequiredPriv: Array[Privilege]): Unit = {}

  override def authorize(table: Table, partition: Partition, list: util.List[String], inputRequiredPriv: Array[Privilege], outputRequiredPriv: Array[Privilege]): Unit = {}

  override def init(configuration: Configuration): Unit = {
    this.hive_db = new HiveProxy(Hive.get(configuration, classOf[HiveAuthorizationProvider]))
  }

  def getCollectArray(inputRequired: Array[Privilege], outputRequired: Array[Privilege]): Array[Privilege] = {
    if (inputRequired == null) {
      outputRequired
    } else if (outputRequired == null) {
      inputRequired
    } else {
      inputRequired ++ outputRequired
    }
  }

  private def getUserName: String = {
    val ss: SessionState = SessionState.get
    if (ss != null && ss.getAuthenticator != null) {
      return ss.getAuthenticator.getUserName
    }
    return null
  }
}

object HiveURMAuthorizationProvider {

  /**
   * authorize any table/database
   * @param sem
   * @param command
   */
  def doAuthorization(sem: BaseSemanticAnalyzer, command: String): Unit = {
    val inputs = sem.getInputs.asScala
    val outputs = sem.getOutputs.asScala
    val ss = SessionState.get()
    val op = ss.getHiveOperation
    if (op == null) {
      throw new HiveException("Operation should not be null")
    }
    val authorizer = SessionState.get().getAuthorizer
    if ((op == HiveOperation.SHOW_GRANT) ||
      (op == HiveOperation.SHOW_ROLE_GRANT) || (op == HiveOperation.CREATEROLE) ||
      (op == HiveOperation.GRANT_PRIVILEGE) || (op == HiveOperation.GRANT_ROLE) ||
      (op == HiveOperation.REVOKE_PRIVILEGE) || (op == HiveOperation.REVOKE_ROLE)) {
      throw new AuthorizationException("About permissions management. Please visit the URM website")
    }
    // outputs table/database authorization
    if (outputs != null && outputs.size > 0) {
      for (write: WriteEntity <- outputs) {
        if (!write.isDummy && !write.isPathType) {
          if (write.getType == Entity.Type.DATABASE) {
            authorizer.authorize(write.getDatabase, null, op.getOutputRequiredPrivileges)
          } else if (write.getTable != null) {
            authorizer.authorize(write.getTable, null, op.getOutputRequiredPrivileges)
          }
        }
      }
    }
    // inputs table/database authorization
    val tableAuthChecked = HashSet[String]()
    if (inputs != null && outputs.size > 0) {
      for (read: ReadEntity <- inputs) {
        if (!read.isDummy && !read.isPathType) {
          if (read.getType == Entity.Type.DATABASE) {
            authorizer.authorize(read.getDatabase, op.getInputRequiredPrivileges, null)
          } else {
            val tbl = read.getTable
            if (tbl != null && !tableAuthChecked.contains(tbl.getTableName)) {
              authorizer.authorize(tbl, op.getInputRequiredPrivileges(), null)
              tableAuthChecked += tbl.getTableName
            }
          }
        }
      }
    }

  }
}
