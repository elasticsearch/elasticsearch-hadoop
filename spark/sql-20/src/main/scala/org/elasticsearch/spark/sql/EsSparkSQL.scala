/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.acc.EsSparkAccumulators
import org.elasticsearch.spark.cfg.SparkSettingsManager

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

@deprecated("Support for Apache Spark 2 is deprecated. Use Spark 3.")
object EsSparkSQL {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient private[this] val LOG = LogFactory.getLog(EsSparkSQL.getClass)

  //
  // Read
  //
  
  def esDF(sc: SQLContext): DataFrame = esDF(sc, Map.empty[String, String])
  def esDF(sc: SQLContext, resource: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource))
  def esDF(sc: SQLContext, resource: String, query: String): DataFrame = esDF(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val esConf = new SparkSettingsManager().load(sc.sparkContext.getConf).copy()
    esConf.merge(cfg.asJava)

    sc.read.format("org.elasticsearch.spark.sql").options(esConf.asProperties.asScala.toMap).load
  }

  def esDF(sc: SQLContext, resource: String, query: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  }

  def esDF(sc: SQLContext, resource: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource))
  }

  // SparkSession variant
  def esDF(ss: SparkSession): DataFrame = esDF(ss.sqlContext, Map.empty[String, String])
  def esDF(ss: SparkSession, resource: String): DataFrame = esDF(ss.sqlContext, Map(ES_RESOURCE_READ -> resource))
  def esDF(ss: SparkSession, resource: String, query: String): DataFrame = esDF(ss.sqlContext, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esDF(ss: SparkSession, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, cfg) 
  def esDF(ss: SparkSession, resource: String, query: String, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, resource, query, cfg)
  def esDF(ss: SparkSession, resource: String, cfg: Map[String, String]): DataFrame = esDF(ss.sqlContext, resource, cfg)
  
  //
  // Write
  //
  
  def saveToEs(srdd: Dataset[_], resource: String): Unit = {
    saveToEs(srdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: Dataset[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: Dataset[_], cfg: Map[String, String]): Unit = {
    if (srdd != null) {
      if (srdd.isStreaming) {
        throw new EsHadoopIllegalArgumentException("Streaming Datasets should not be saved with 'saveToEs()'. Instead, use " +
          "the 'writeStream().format(\"es\").save()' methods.")
      }
      val sparkCtx = srdd.sqlContext.sparkContext
      val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
      val esCfg = new PropertiesSettings().load(sparkCfg.save())
      esCfg.merge(cfg.asJava)
      EsSparkAccumulators.build(sparkCtx, esCfg.getMetricsPrefix)

      // Need to discover ES Version before checking index existence
      InitializationUtils.setUserProviderIfNotSet(esCfg, classOf[HadoopUserProvider], LOG)
      InitializationUtils.discoverClusterInfo(esCfg, LOG)
      InitializationUtils.checkIdForOperation(esCfg)
      InitializationUtils.checkIndexExistence(esCfg)

      sparkCtx.runJob(srdd.toDF().rdd, new EsDataFrameWriter(srdd.schema, esCfg.save()).write _)
    }
  }
}