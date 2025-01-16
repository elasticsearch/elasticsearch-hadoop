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
package org.elasticsearch.spark.rdd

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OUTPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.spark.acc.EsSparkAccumulators

object EsSpark {

  @transient private[this] val LOG = LogFactory.getLog(EsSpark.getClass)

  //
  // Load methods
  //

  def esRDD(sc: SparkContext): RDD[(String, Map[String, AnyRef])] = new ScalaEsRDD[Map[String, AnyRef]](sc)
  def esRDD(sc: SparkContext, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaEsRDD[Map[String, AnyRef]](sc, cfg)
  def esRDD(sc: SparkContext, resource: String): RDD[(String, Map[String, AnyRef])] =
    new ScalaEsRDD[Map[String, AnyRef]](sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SparkContext, resource: String, query: String): RDD[(String, Map[String, AnyRef])] =
    new ScalaEsRDD[Map[String, AnyRef]](sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(sc: SparkContext, resource: String, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaEsRDD[Map[String, AnyRef]](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource))
  def esRDD(sc: SparkContext, resource: String, query: String, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaEsRDD[Map[String, AnyRef]](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query))


  // load data as JSON
  def esJsonRDD(sc: SparkContext): RDD[(String, String)] = new ScalaEsRDD[String](sc, Map(ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaEsRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String): RDD[(String, String)] =
    new ScalaEsRDD[String](sc, Map(ES_RESOURCE_READ -> resource, ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, query: String): RDD[(String, String)] =
    new ScalaEsRDD[String](sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query, ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaEsRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, query: String, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaEsRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_READ -> resource, ES_QUERY -> query, ES_OUTPUT_JSON -> true.toString))


  //
  // Save methods
  //
  def saveToEs(rdd: RDD[_], resource: String): Unit = { saveToEs(rdd, Map(ES_RESOURCE_WRITE -> resource)) }
  def saveToEs(rdd: RDD[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(rdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(rdd: RDD[_], cfg: Map[String, String]): Unit =  {
    doSaveToEs(rdd, cfg, false)
  }

  // Save with metadata
  def saveToEsWithMeta[K,V](rdd: RDD[(K,V)], resource: String): Unit = { saveToEsWithMeta(rdd, Map(ES_RESOURCE_WRITE -> resource)) }
  def saveToEsWithMeta[K,V](rdd: RDD[(K,V)], resource: String, cfg: Map[String, String]): Unit = {
    saveToEsWithMeta(rdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEsWithMeta[K,V](rdd: RDD[(K,V)], cfg: Map[String, String]): Unit = {
    doSaveToEs(rdd, cfg, true)
  }

  private[spark] def doSaveToEs(rdd: RDD[_], cfg: Map[String, String], hasMeta: Boolean): Unit = {
    CompatUtils.warnSchemaRDD(rdd, LogFactory.getLog("org.elasticsearch.spark.rdd.EsSpark"))

    if (rdd == null || rdd.partitions.length == 0) {
      return
    }

    val sc = rdd.sparkContext
    val sparkCfg = new SparkSettingsManager().load(sc.getConf)
    val config = new PropertiesSettings().load(sparkCfg.save())
    config.merge(cfg.asJava)
    EsSparkAccumulators.build(sc, config.getMetricsPrefix)

    // Need to discover the EsVersion here before checking if the index exists
    InitializationUtils.setUserProviderIfNotSet(config, classOf[HadoopUserProvider], LOG)
    InitializationUtils.discoverClusterInfo(config, LOG)
    InitializationUtils.checkIdForOperation(config)
    InitializationUtils.checkIndexExistence(config)

    rdd.sparkContext.runJob(rdd, new EsRDDWriter(config.save(), hasMeta).write _)
  }

  // JSON variant
  def saveJsonToEs(rdd: RDD[_], resource: String): Unit = { saveToEs(rdd, resource, Map(ES_INPUT_JSON -> true.toString)) }
  def saveJsonToEs(rdd: RDD[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(rdd, resource, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(rdd: RDD[_], cfg: Map[String, String]): Unit = {
    saveToEs(rdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_INPUT_JSON -> true.toString))
  }
}
