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

package org.apache.spark.deploy.yarn

import java.io.File
import java.net.URI
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import org.apache.spark.{SparkConf, Logging}
import org.apache.hadoop.yarn.conf.YarnConfiguration


trait ExecutorRunnableUtil extends Logging {

  val yarnConf: YarnConfiguration
  val sparkConf: SparkConf
  lazy val env = prepareEnvironment

  def prepareCommand(
      masterAddress: String,
      slaveId: String,
      hostname: String,
      executorMemory: Int,
      executorCores: Int,
      localResources: HashMap[String, LocalResource]) = {
    // Extra options for the JVM
    var JAVA_OPTS = ""
    // Set the JVM memory
    val executorMemoryString = executorMemory + "m"
    JAVA_OPTS += "-Xms" + executorMemoryString + " -Xmx" + executorMemoryString + " "
    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }

    JAVA_OPTS += " -Djava.io.tmpdir=" +
      new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR) + " "
    JAVA_OPTS += ClientBase.getLog4jConfiguration(localResources)

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence
    // if there are multiple containers in same node, spark gc effects all other containers
    // performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    /*
        else {
          // If no java_opts specified, default to using -XX:+CMSIncrementalMode
          // It might be possible that other modes/config is being done in SPARK_JAVA_OPTS, so we dont
          // want to mess with it.
          // In our expts, using (default) throughput collector has severe perf ramnifications in
          // multi-tennent machines
          // The options are based on
          // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use%20the%20Concurrent%20Low%20Pause%20Collector|outline
          JAVA_OPTS += " -XX:+UseConcMarkSweepGC "
          JAVA_OPTS += " -XX:+CMSIncrementalMode "
          JAVA_OPTS += " -XX:+CMSIncrementalPacing "
          JAVA_OPTS += " -XX:CMSIncrementalDutyCycleMin=0 "
          JAVA_OPTS += " -XX:CMSIncrementalDutyCycle=10 "
        }
    */

    val commands = List[String](
      Environment.JAVA_HOME.$() + "/bin/java" +
      " -server " +
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      // Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
      // an inconsistent state.
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do
      // 'something' to fail job ... akin to blacklisting trackers in mapred ?
      " -XX:OnOutOfMemoryError='kill %p' " +
      JAVA_OPTS +
      " org.apache.spark.executor.CoarseGrainedExecutorBackend " +
      masterAddress + " " +
      slaveId + " " +
      hostname + " " +
      executorCores +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    commands
  }

  private def setupDistributedCache(
      file: String,
      rtype: LocalResourceType,
      localResources: HashMap[String, LocalResource],
      timestamp: String,
      size: String,
      vis: String) = {
    val uri = new URI(file)
    val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    amJarRsrc.setType(rtype)
    amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
    amJarRsrc.setTimestamp(timestamp.toLong)
    amJarRsrc.setSize(size.toLong)
    localResources(uri.getFragment()) = amJarRsrc
  }

  def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val localResources = HashMap[String, LocalResource]()

    if (System.getenv("SPARK_YARN_CACHE_FILES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_FILES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_FILES_FILE_SIZES").split(',')
      val distFiles = System.getenv("SPARK_YARN_CACHE_FILES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_FILES_VISIBILITIES").split(',')
      for( i <- 0 to distFiles.length - 1) {
        setupDistributedCache(distFiles(i), LocalResourceType.FILE, localResources, timeStamps(i),
          fileSizes(i), visibilities(i))
      }
    }

    if (System.getenv("SPARK_YARN_CACHE_ARCHIVES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES").split(',')
      val distArchives = System.getenv("SPARK_YARN_CACHE_ARCHIVES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES").split(',')
      for( i <- 0 to distArchives.length - 1) {
        setupDistributedCache(distArchives(i), LocalResourceType.ARCHIVE, localResources,
          timeStamps(i), fileSizes(i), visibilities(i))
      }
    }

    logInfo("Prepared Local resources " + localResources)
    localResources
  }

  def prepareEnvironment: HashMap[String, String] = {
    val env = new HashMap[String, String]()

    val log4jConf = System.getenv(ClientBase.LOG4J_CONF_ENV_KEY)
    ClientBase.populateClasspath(null, yarnConf, sparkConf, log4jConf, env)
    if (log4jConf != null) {
      env(ClientBase.LOG4J_CONF_ENV_KEY) = log4jConf
    }

    // Allow users to specify some environment variables
    YarnSparkHadoopUtil.setEnvFromInputString(env, System.getenv("SPARK_YARN_USER_ENV"),
      File.pathSeparator)

    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    env
  }

}
