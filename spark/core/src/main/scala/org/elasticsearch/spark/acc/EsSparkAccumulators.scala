package org.elasticsearch.spark.acc

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import org.elasticsearch.hadoop.mr.Counter
import org.elasticsearch.hadoop.rest.stats.Stats

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable

object EsSparkAccumulators {

  def getAll: Set[LongAccumulator] = accumulators.values.toSet

  private val accumulators: mutable.Map[String, LongAccumulator] = mutable.Map[String, LongAccumulator]()

  private val counters = Counter.ALL.asScala

  private[spark] def build(sc: SparkContext, prefix: String): Unit = {
    for (counter <- counters) {
      val fullName = getAccumulatorFullName(prefix, counter.name())
      if(!accumulators.contains(fullName)) {
        accumulators += (fullName -> sc.longAccumulator(fullName))
      }
    }
  }

  private[spark] def add(stats: Stats, prefix: String): Unit = {
    if (accumulators.nonEmpty) {
      for (counter <- counters) {
        accumulators.get(getAccumulatorFullName(prefix, counter.name())).foreach(_.add(counter.get(stats)))
      }
    }
  }

  private[spark] def get(prefix: String, counter: Counter): Long = {
    accumulators(getAccumulatorFullName(prefix, counter.name())).value
  }

  private[spark] def clear(): Unit = {
    accumulators.clear()
  }

  private def getAccumulatorFullName(prefix: String, name: String): String = {
    val prefixWithSep = if (prefix.isEmpty) "" else s"${prefix}."
    s"${prefixWithSep}${name}"
  }

}
