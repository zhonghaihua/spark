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


package org.apache.spark.sql.hive.orc

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Locale, Date}
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, FileOutputCommitter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.{Writable, NullWritable}
import org.apache.hadoop.mapreduce.{TaskID, TaskAttemptContext, Job}
import org.apache.hadoop.hive.ql.io.orc.{OrcSerde, OrcInputFormat, OrcOutputFormat}
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.mapred.{SparkHadoopMapRedUtil, Reporter, JobConf}

import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.FileSystemHelper
import org.apache.spark.{TaskContext, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode => LogicalUnaryNode}
import org.apache.spark.sql.execution.UnaryNode
import org.apache.spark.sql.hive.HadoopTableReader

import scala.collection.JavaConversions._

/**
 * logical plan of writing to ORC file
 */
case class WriteToOrcFile(
    path: String,
    child: LogicalPlan) extends LogicalUnaryNode {
  def output = child.output
}

/**
 * orc table scan operator. Imports the file that backs the given
 * [[org.apache.spark.sql.hive.orc.OrcRelation]] as a ``RDD[Row]``.
 */
case class OrcTableScan(
    output: Seq[Attribute],
    relation: OrcRelation,
    columnPruningPred: Option[Expression])
  extends LeafNode {

  @transient
  lazy val serde: OrcSerde = initSerde

  private def initSerde(): OrcSerde = {
    val serde = new OrcSerde
    serde.initialize(null, relation.prop)
    serde
  }

  override def execute(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)

    val conf: Configuration = job.getConfiguration
    val fileList = FileSystemHelper.listFiles(relation.path, conf)

    // add all paths in the directory but skip "hidden" ones such
    // as "_SUCCESS"
    for (path <- fileList if !path.getName.startsWith("_")) {
      FileInputFormat.addInputPath(job, path)
    }

    addColumnIds(output, relation, conf)
    val inputClass = classOf[OrcInputFormat].asInstanceOf[
      Class[_ <: org.apache.hadoop.mapred.InputFormat[NullWritable, Writable]]]

    // use SpecificMutableRow to decrease GC garbage
    val mutableRow = new SpecificMutableRow(output.map(_.dataType))
    val attrsWithIndex = output.zipWithIndex
    val rowRdd = sc.hadoopRDD[NullWritable, Writable](conf.asInstanceOf[JobConf], inputClass,
      classOf[NullWritable], classOf[Writable]).map(_._2).mapPartitions { iter =>
      val deserializer = serde
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow)
    }
    rowRdd
  }

  /**
   * add column ids and names
   * @param output
   * @param relation
   * @param conf
   */
  def addColumnIds(output: Seq[Attribute], relation: OrcRelation, conf: Configuration) {
    val fieldIdMap = relation.output.map(_.name).zipWithIndex.toMap

    val ids = output.map(att => {
      val realName = att.name.toLowerCase(Locale.ENGLISH)
      fieldIdMap.getOrElse(realName, -1)
    }).filter(_ >= 0)
    if (ids != null && !ids.isEmpty) {
      ColumnProjectionUtils.appendReadColumnIDs(conf, ids.asInstanceOf[java.util.List[Integer]])
    }

    val names = output.map(_.name)
    if (names != null && !names.isEmpty) {
      ColumnProjectionUtils.appendReadColumnNames(conf, names)
    }
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): OrcTableScan = {
    OrcTableScan(prunedAttributes, relation, columnPruningPred)
  }
}

/**
 * Operator that acts as a sink for queries on RDDs and can be used to
 * store the output inside a directory of ORC files. This operator
 * is similar to Hive's INSERT INTO TABLE operation in the sense that
 * one can choose to either overwrite or append to a directory. Note
 * that consecutive insertions to the same table must have compatible
 * (source) schemas.
 */
private[sql] case class InsertIntoOrcTable(
    relation: OrcRelation,
    child: SparkPlan,
    overwrite: Boolean = false)
  extends UnaryNode with SparkHadoopMapRedUtil with org.apache.spark.Logging {

  override def output = child.output

  val inputClass = getInputClass.getName

  @transient val sc = sqlContext.sparkContext

  @transient lazy val orcSerde = initFieldInfo

  private def getInputClass: Class[_] = {
    val existRdd = child.asInstanceOf[PhysicalRDD]
    val productClass = existRdd.rdd.firstParent.elementClassTag.runtimeClass
    logInfo("productClass is " + productClass)
    val clazz = productClass
    if (null == relation.rowClass) {
      relation.rowClass = clazz
    }
    clazz
  }

  private def getInspector(clazz: Class[_]): ObjectInspector = {
    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(clazz,
      ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
    inspector
  }

  private def initFieldInfo(): OrcSerde = {
    val serde: OrcSerde = new OrcSerde
    serde
  }

  /**
   * Inserts all rows into the Orc file.
   */
  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    if (overwrite) {
      try {
        fs.delete(fspath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${fspath.toString} prior"
              + s" to InsertIntoOrcTable:\n${e.toString}")
      }
    }

    val existRdd = child.asInstanceOf[PhysicalRDD]
    val parentRdd = existRdd.rdd.firstParent[Product]
    val writableRdd = parentRdd.mapPartitions { iter =>
      val objSnspector = ObjectInspectorFactory.getReflectionObjectInspector(
        getContextOrSparkClassLoader.loadClass(inputClass),
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
      iter.map(obj => orcSerde.serialize(obj, objSnspector))
    }

    saveAsHadoopFile(writableRdd, relation.rowClass, relation.path, conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }


  // based on ``saveAsNewAPIHadoopFile`` in [[PairRDDFunctions]]
  // TODO: Maybe PairRDDFunctions should use Product2 instead of Tuple2?
  // .. then we could use the default one and could use [[MutablePair]]
  // instead of ``Tuple2``
  private def saveAsHadoopFile(
      rdd: RDD[Writable],
      rowClass: Class[_],
      path: String,
      @transient conf: Configuration) {
    val job = new Job(conf)
    val keyType = classOf[Void]
    job.setOutputKeyClass(keyType)
    job.setOutputValueClass(classOf[Writable])
    FileOutputFormat.setOutputPath(job, new Path(path))

    val wrappedConf = new SerializableWritable(job.getConfiguration)

    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset =
      if (overwrite) {
        1
      } else {
        FileSystemHelper
          .findMaxTaskId(
            FileOutputFormat.getOutputPath(job).toString, job.getConfiguration, "orc") + 1
      }

    def getWriter(
                   outFormat: OrcOutputFormat,
                   conf: Configuration,
                   path: Path,
                   reporter: Reporter) = {
      val fs = path.getFileSystem(conf)
      outFormat.getRecordWriter(fs, conf.asInstanceOf[JobConf], path.toUri.getPath, reporter).
        asInstanceOf[org.apache.hadoop.mapred.RecordWriter[NullWritable, Writable]]
    }

    def getCommitterAndWriter(offset: Int, context: TaskAttemptContext) = {
      val outFormat = new OrcOutputFormat

      val taskId: TaskID = context.getTaskAttemptID.getTaskID
      val partition: Int = taskId.getId
      val filename = s"part-r-${partition + offset}.orc"
      val output: Path = FileOutputFormat.getOutputPath(context)
      val committer = new FileOutputCommitter(output, context)
      val path = new Path(committer.getWorkPath, filename)
      val writer = getWriter(outFormat, wrappedConf.value, path, Reporter.NULL)
      (committer, writer)
    }

    def writeShard(context: TaskContext, iter: Iterator[Writable]): Int = {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value.asInstanceOf[JobConf], attemptId)
      val workerAndComitter = getCommitterAndWriter(taskIdOffset, hadoopContext)
      val writer = workerAndComitter._2

      while (iter.hasNext) {
        val row = iter.next()
        writer.write(NullWritable.get(), row)
      }

      writer.close(Reporter.NULL)
      workerAndComitter._1.commitTask(hadoopContext)
      return 1
    }

    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(
      wrappedConf.value.asInstanceOf[JobConf], jobAttemptId)
    val workerAndComitter = getCommitterAndWriter(taskIdOffset, jobTaskContext)
    workerAndComitter._1.setupJob(jobTaskContext)
    sc.runJob(rdd, writeShard _)
    workerAndComitter._1.commitJob(jobTaskContext)
  }
}
