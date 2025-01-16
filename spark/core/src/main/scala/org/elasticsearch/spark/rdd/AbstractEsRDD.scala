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
package org.elasticsearch.spark.rdd;

import JDKCollectionConvertersCompat.Converters._

import scala.reflect.ClassTag
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.rest.PartitionDefinition
import org.elasticsearch.hadoop.util.ObjectUtils
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.hadoop.rest.RestRepository
import org.elasticsearch.spark.acc.EsSparkAccumulators

import scala.annotation.meta.param

private[spark] abstract class AbstractEsRDD[T: ClassTag](
  @(transient @param) sc: SparkContext,
  val params: scala.collection.Map[String, String] = Map.empty)
  extends RDD[T](sc, Nil) {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient protected lazy val logger = LogFactory.getLog(this.getClass())

  override def getPartitions: Array[Partition] = {
    esPartitions.asScala.zipWithIndex.map { case(esPartition, idx) =>
      new EsPartition(id, idx, esPartition)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val esSplit = split.asInstanceOf[EsPartition]
    esSplit.esPartition.getHostNames
  }

  override def checkpoint(): Unit = {
    // Do nothing. Elasticsearch RDD should not be checkpointed.
  }

  def esCount(): Long = {
    val repo = new RestRepository(esCfg)
    try {
      repo.count(true)
    } finally {
      repo.close()
    }
  }

  EsSparkAccumulators.build(sc, esCfg.getMetricsPrefix)

  @transient private[spark] lazy val esCfg = {
    val cfg = new SparkSettingsManager().load(sc.getConf).copy();
    cfg.merge(params.asJava)
    InitializationUtils.setUserProviderIfNotSet(cfg, classOf[HadoopUserProvider], logger)
    cfg
  }

  @transient private[spark] lazy val esPartitions = {
    RestService.findPartitions(esCfg, logger)
  }
}

private[spark] class EsPartition(rddId: Int, idx: Int, val esPartition: PartitionDefinition)
  extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx) + esPartition.hashCode()

  override val index: Int = idx
}
