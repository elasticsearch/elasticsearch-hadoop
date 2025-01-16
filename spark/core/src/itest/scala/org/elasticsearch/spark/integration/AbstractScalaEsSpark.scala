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
package org.elasticsearch.spark.integration;

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.{lang => jl}
import java.{util => ju}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.elasticsearch.hadoop.EsAssume
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.TestData
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_AUTO_CREATE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INPUT_JSON
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_EXCLUDE
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_ID
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_JOIN
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_READ_METADATA
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE
import org.elasticsearch.hadoop.util.TestUtils.resource
import org.elasticsearch.hadoop.util.TestUtils.docEndpoint
import org.elasticsearch.hadoop.rest.RestUtils
import org.elasticsearch.hadoop.rest.RestUtils.ExtendedRestClient
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException
import org.elasticsearch.hadoop.util.EsMajorVersion
import org.elasticsearch.hadoop.util.StringUtils
import org.elasticsearch.hadoop.util.TestSettings
import org.elasticsearch.hadoop.util.TestUtils
import org.elasticsearch.spark.acc.EsSparkAccumulators
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata.ID
import org.elasticsearch.spark.rdd.Metadata.TTL
import org.elasticsearch.spark.rdd.Metadata.VERSION
import org.elasticsearch.spark.serialization.Bean
import org.elasticsearch.spark.serialization.ReflectionUtils
import org.elasticsearch.spark.sparkByteArrayJsonRDDFunctions
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.sparkPairRDDFunctions
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sparkStringJsonRDDFunctions
import org.hamcrest.Matchers.both
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Assume.assumeNoException
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import org.elasticsearch.spark.integration.ScalaUtils.propertiesAsScalaMap
import org.elasticsearch.spark.rdd.JDKCollectionConvertersCompat.Converters._

object AbstractScalaEsScalaSpark {
  @transient val conf = new SparkConf()
              .setAppName("estest")
              .set("spark.io.compression.codec", "lz4")
              .setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null

  @ClassRule @transient val testData = new TestData()

  @BeforeClass
  def setup() {
    conf.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
    sc = new SparkContext(conf)
  }

  @AfterClass
  def cleanup() {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("default-", jl.Boolean.FALSE))
    list.add(Array("with-meta-", jl.Boolean.TRUE))
    list
  }

  case class ModuleCaseClass(id: Integer, departure: String, var arrival: String) {
    var l = math.Pi
  }
}

case class Trip(departure: String, arrival: String) {
  var extra = math.Pi
}

class Garbage(i: Int) {
  def doNothing(): Unit = ()
}

@RunWith(classOf[Parameterized])
class AbstractScalaEsScalaSpark(prefix: String, readMetadata: jl.Boolean) extends Serializable {

  val sc = AbstractScalaEsScalaSpark.sc
  val cfg = Map(ES_READ_METADATA -> readMetadata.toString())
  val version: EsMajorVersion = TestUtils.getEsClusterInfo.getMajorVersion
  val keyword: String = if (version.onOrAfter(EsMajorVersion.V_5_X)) "keyword" else "string"

  private def readAsRDD(uri: URI) = {
    // don't use the sc.read.json/textFile to avoid the whole Hadoop madness
    val path = Paths.get(uri)
    // because Windows
    val lines: Seq[String] = Files.readAllLines(path, StandardCharsets.ISO_8859_1).asScala.toSeq
    sc.parallelize(lines)
  }
  
  @Test
  def testBasicRead() {
    val input = AbstractScalaEsScalaSpark.testData.sampleArtistsDatUri()
    val data = readAsRDD(input).cache()

    assertTrue(data.count > 300)
  }

  @Test
  def testRDDEmptyRead() {
    val target = wrapIndex(resource("spark-test-empty-rdd", "data", version))
    sc.emptyRDD.saveToEs(target, cfg)
  }

  @Test(expected=classOf[EsHadoopIllegalArgumentException])
  def testEsRDDWriteIndexCreationDisabled() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-nonexisting-scala-basic-write", "data", version))

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, collection.mutable.Map(cfg.toSeq: _*) += (
      ES_INDEX_AUTO_CREATE -> "no"))
    assertTrue(!RestUtils.exists(target))
  }
    
  @Test
  def testEsRDDWrite() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-scala-basic-write", "data", version))

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, cfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test(expected = classOf[SparkException])
  def testNestedUnknownCharacter() {
    val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Garbage(0))
    sc.makeRDD(Seq(doc)).saveToEs(wrapIndex(resource("spark-test-nested-map", "data", version)), cfg)
  }

  @Test
  def testEsRDDWriteCaseClass() {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = AbstractScalaEsScalaSpark.ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)

     val target = wrapIndex(resource("spark-test-scala-basic-write-objects", "data", version))

    sc.makeRDD(Seq(javaBean, caseClass1)).saveToEs(target, cfg)
    sc.makeRDD(Seq(javaBean, caseClass2)).saveToEs(target, Map("es.mapping.id"->"id"))

    assertTrue(RestUtils.exists(target))
    assertEquals(3, EsSpark.esRDD(sc, target).count())
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test
  def testEsRDDWriteWithMappingId() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = resource("spark-test-scala-id-write", "data", version)
    val docPath = docEndpoint("spark-test-scala-id-write", "data", version)

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, Map(ES_MAPPING_ID -> "number"))

    assertEquals(2, EsSpark.esRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/1"))
    assertTrue(RestUtils.exists(docPath + "/2"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }
  
  @Test
  def testEsRDDWriteWithDynamicMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-dyn-id-write", "data", version))

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2))).saveToEsWithMeta(target, cfg)

    assertEquals(2, EsSpark.esRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/3"))
    assertTrue(RestUtils.exists(docPath + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testEsRDDWriteWithDynamicMapMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-dyn-id-write", "data", version))

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    pairRDD.saveToEsWithMeta(target, cfg)

    assertTrue(RestUtils.exists(docPath + "/5"))
    assertTrue(RestUtils.exists(docPath + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test(expected = classOf[EsHadoopSerializationException])
  def testEsRDDWriteWithUnsupportedMapping() {
    EsAssume.versionOnOrAfter(EsMajorVersion.V_6_X, "TTL only removed in v6 and up.")

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write-fail", "data", version))

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, TTL -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    try {
      pairRDD.saveToEsWithMeta(target, cfg)
    } catch {
      case s: SparkException => throw s.getCause
      case t: Throwable => throw t
    }

    fail("Should not have ingested TTL on ES 6.x+")
  }

  @Test
  def testEsRDDWriteWithMappingExclude() {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex(resource("spark-test-scala-write-exclude", "data", version))

    sc.makeRDD(Seq(trip1, trip2)).saveToEs(target, Map(ES_MAPPING_EXCLUDE -> "airport"))
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
  }

  @Test
  def testEsRDDWriteJoinField(): Unit = {
    // Join added in 6.0.
    // TODO: Available in 5.6, but we only track major version ids in the connector.
    EsAssume.versionOnOrAfter(EsMajorVersion.V_6_X, "Join added in 6.0.")

    // test mix of short-form and long-form joiner values
    val company1 = Map("id" -> "1", "company" -> "Elastic", "joiner" -> "company")
    val company2 = Map("id" -> "2", "company" -> "Fringe Cafe", "joiner" -> Map("name" -> "company"))
    val company3 = Map("id" -> "3", "company" -> "WATIcorp", "joiner" -> Map("name" -> "company"))

    val employee1 = Map("id" -> "10", "name" -> "kimchy", "joiner" -> Map("name" -> "employee", "parent" -> "1"))
    val employee2 = Map("id" -> "20", "name" -> "April Ryan", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee3 = Map("id" -> "21", "name" -> "Charlie", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee4 = Map("id" -> "30", "name" -> "Alvin Peats", "joiner" -> Map("name" -> "employee", "parent" -> "3"))

    val parents = Seq(company1, company2, company3)
    val children = Seq(employee1, employee2, employee3, employee4)
    val docs = parents ++ children

    {
      val index = wrapIndex("spark-test-scala-write-join-separate")
      val typename = "join"
      val target = resource(index, typename, version)
      val getEndpoint = docEndpoint(index, typename, version)

      if (TestUtils.isTypelessVersion(version)) {
        RestUtils.putMapping(index, typename, "data/join/mapping/typeless.json")
      } else {
        RestUtils.putMapping(index, typename, "data/join/mapping/typed.json")
      }

      sc.makeRDD(parents).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))
      sc.makeRDD(children).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.esRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }

    }
    {
      val index = wrapIndex("spark-test-scala-write-join-combined")
      val typename = "join"
      val target = resource(index, typename, version)
      val getEndpoint = docEndpoint(index, typename, version)

      if (TestUtils.isTypelessVersion(version)) {
        RestUtils.putMapping(index, typename, "data/join/mapping/typeless.json")
      } else {
        RestUtils.putMapping(index, typename, "data/join/mapping/typed.json")
      }

      sc.makeRDD(docs).saveToEs(target, Map(ES_MAPPING_ID -> "id", ES_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.esRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }
    }
  }

  @Test
  def testEsRDDIngest() {
    EsAssume.versionOnOrAfter(EsMajorVersion.V_5_X, "Ingest Supported in 5.x and above only")

    val client: RestUtils.ExtendedRestClient = new RestUtils.ExtendedRestClient
    val prefix: String = "spark"
    val pipeline: String = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}"
    client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline))
    client.close();

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-scala-ingest-write", "data", version))

    val ingestCfg = cfg + (ConfigurationOptions.ES_INGEST_PIPELINE -> "spark-pipeline") + (ConfigurationOptions.ES_NODES_INGEST_ONLY -> "true")

    sc.makeRDD(Seq(doc1, doc2)).saveToEs(target, ingestCfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
    assertThat(RestUtils.get(target + "/_search?"), containsString("\"pipeTEST\":true"))
  }


  @Test
  def testEsMultiIndexRDDWrite() {
    val trip1 = Map("reason" -> "business", "airport" -> "sfo")
    val trip2 = Map("participants" -> 5, "airport" -> "otp")

    val target = wrapIndex(resource("spark-test-trip-{airport}", "data", version))
    sc.makeRDD(Seq(trip1, trip2)).saveToEs(target, cfg)
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-trip-otp", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-trip-sfo", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-test-trip-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-test-trip-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test
  def testEsWriteAsJsonMultiWrite() {
    val json1 = "{\"reason\" : \"business\",\"airport\" : \"sfo\"}";
    val json2 = "{\"participants\" : 5,\"airport\" : \"otp\"}"

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs(wrapIndex(resource("spark-test-json-{airport}", "data", version)), cfg)

    val json1BA = json1.getBytes()
    val json2BA = json2.getBytes()

    sc.makeRDD(Seq(json1BA, json2BA)).saveJsonToEs(wrapIndex(resource("spark-test-json-ba-{airport}", "data", version)), cfg)

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-otp", "data", version))))

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-ba-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-ba-otp", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-test-json-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-test-json-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test(expected = classOf[EsHadoopIllegalArgumentException ])
  def testEsRDDBreakOnFileScript(): Unit = {
    EsAssume.versionOnOrAfter(EsMajorVersion.V_6_X, "File scripts are only removed in 6.x and on")
    val props = Map("es.write.operation" -> "upsert", "es.update.script.file" -> "break")
    val lines = sc.makeRDD(List(Map("id" -> "1")))
    try {
      lines.saveToEs("should-break", props)
    } catch {
      case s: SparkException => throw s.getCause
      case t: Throwable => throw t
    }
    fail("Should not have succeeded with file script on ES 6x and up.")
  }

  @Test
  def testEsRDDWriteFileScriptUpdate(): Unit = {
    EsAssume.versionOnOrBefore(EsMajorVersion.V_5_X, "File scripts are only available in 5.x and lower")
    // assumes you have a script named "increment" as a file. I don't think there's a way to verify this before
    // the test runs. Maybe a quick poke before the job runs?

    val mapping = wrapMapping("data",
      s"""{
         |    "properties": {
         |      "id": {
         |        "type": "$keyword"
         |      },
         |      "counter": {
         |        "type": "long"
         |      }
         |    }
         |}""".stripMargin)

    val index = wrapIndex("spark-test-stored")
    RestUtils.touch(index)

    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.putMapping(index, typename, mapping.getBytes)

    RestUtils.refresh(index)
    RestUtils.put(s"$docPath/1", """{"id":"1", "counter":4}""".getBytes(StringUtils.UTF_8))

    // Test assumption:
    try {
      if (version.onOrBefore(EsMajorVersion.V_2_X)) {
        RestUtils.postData(s"$target/1/_update", """{"script_file":"increment"}""".getBytes(StringUtils.UTF_8))
      } else if (TestUtils.isTypelessVersion(version)) {
        RestUtils.postData(s"$index/_update/1", """{"script": { "file":"increment" } }""".getBytes(StringUtils.UTF_8))
      } else {
        RestUtils.postData(s"$target/1/_update", """{"script": { "file":"increment" } }""".getBytes(StringUtils.UTF_8))
      }
    } catch {
      case t: Throwable => assumeNoException("Script not installed", t)
    }

    val scriptName = "increment"
    val lang = if (version.onOrAfter(EsMajorVersion.V_5_X)) "painless" else "groovy"
    val script = if (version.onOrAfter(EsMajorVersion.V_5_X)) {
      "ctx._source.counter = ctx._source.getOrDefault('counter', 0) + 1"
    } else {
      "ctx._source.counter += 1"
    }

    val props = Map("es.write.operation" -> "update", "es.mapping.id" -> "id", "es.update.script.file" -> scriptName)
    val lines = sc.makeRDD(List(Map("id"->"1")))
    lines.saveToEs(target, props)

    val docs = RestUtils.get(s"$target/_search")
    Assert.assertThat(docs, containsString(""""counter":6"""))
  }

  @Test
  def testEsRDDWriteStoredScriptUpdate(): Unit = {
    val mapping = wrapMapping("data",
      s"""{
        |    "properties": {
        |      "id": {
        |        "type": "$keyword"
        |      },
        |      "counter": {
        |        "type": "long"
        |      }
        |    }
        |}""".stripMargin)

    val index = wrapIndex("spark-test-stored")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.putMapping(index, typename, mapping.getBytes())
    RestUtils.put(s"$docPath/1", """{"id":"1", "counter":5}""".getBytes(StringUtils.UTF_8))

    val scriptName = "increment"
    val lang = if (version.onOrAfter(EsMajorVersion.V_5_X)) "painless" else "groovy"
    val script = if (version.onOrAfter(EsMajorVersion.V_5_X)) {
      "ctx._source.counter = ctx._source.getOrDefault('counter', 0) + 1"
    } else {
      "ctx._source.counter += 1"
    }

    if (version.onOrAfter(EsMajorVersion.V_8_X)) {
      RestUtils.put(s"_scripts/$scriptName", s"""{"script":{"lang":"$lang", "source": "$script"}}""".getBytes(StringUtils.UTF_8))
    } else if (version.onOrAfter(EsMajorVersion.V_5_X)) {
      RestUtils.put(s"_scripts/$scriptName", s"""{"script":{"lang":"$lang", "code": "$script"}}""".getBytes(StringUtils.UTF_8))
    } else {
      RestUtils.put(s"_scripts/$lang/$scriptName", s"""{"script":"$script"}""".getBytes(StringUtils.UTF_8))
    }

    val props = Map("es.write.operation" -> "update", "es.mapping.id" -> "id", "es.update.script.stored" -> scriptName)
    val lines = sc.makeRDD(List(Map("id"->"1")))
    lines.saveToEs(target, props)

    val docs = RestUtils.get(s"$target/_search")
    Assert.assertThat(docs, containsString(""""counter":6"""))
  }

  @Test
  def testEsRDDWriteWithUpsertScriptUsingBothObjectAndRegularString() {
    val mapping = wrapMapping("data", s"""{
                    |    "properties": {
                    |      "id": {
                    |        "type": "$keyword"
                    |      },
                    |      "note": {
                    |        "type": "$keyword"
                    |      },
                    |      "address": {
                    |        "type": "nested",
                    |        "properties": {
                    |          "id":    { "type": "$keyword"  },
                    |          "zipcode": { "type": "$keyword"  }
                    |        }
                    |      }
                    |    }
                    |}""".stripMargin)

    val index = wrapIndex("spark-test-contact")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)

    RestUtils.putMapping(index, typename, mapping.getBytes())
    RestUtils.postData(s"$docPath/1", """{ "id" : "1", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$docPath/2", """{ "id" : "2", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))

    val lang = if (version.onOrAfter(EsMajorVersion.V_5_X)) "painless" else "groovy"
    val props = Map("es.write.operation" -> "upsert",
      "es.input.json" -> "true",
      "es.mapping.id" -> "id",
      "es.update.script.lang" -> lang
    )

    // Upsert a value that should only modify the first document. Modification will add an address entry.
    val lines = sc.makeRDD(List("""{"id":"1","address":{"zipcode":"12345","id":"1"}}"""))
    val up_params = "new_address:address"
    val up_script = {
      if (version.onOrAfter(EsMajorVersion.V_5_X)) {
        "ctx._source.address.add(params.new_address)"
      } else {
        "ctx._source.address+=new_address"
      }
    }
    lines.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script))

    // Upsert a value that should only modify the second document. Modification will update the "note" field.
    val notes = sc.makeRDD(List("""{"id":"2","note":"Second"}"""))
    val note_up_params = "new_note:note"
    val note_up_script = {
      if (version.onOrAfter(EsMajorVersion.V_5_X)) {
        "ctx._source.note = params.new_note"
      } else {
        "ctx._source.note=new_note"
      }
    }
    notes.saveToEs(target, props + ("es.update.script.params" -> note_up_params) + ("es.update.script" -> note_up_script))

    assertTrue(RestUtils.exists(s"$docPath/1"))
    assertThat(RestUtils.get(s"$docPath/1"), both(containsString(""""zipcode":"12345"""")).and(containsString(""""note":"First"""")))

    assertTrue(RestUtils.exists(s"$docPath/2"))
    assertThat(RestUtils.get(s"$docPath/2"), both(not(containsString(""""zipcode":"12345""""))).and(containsString(""""note":"Second"""")))
  }

  @Test
  def testEsRDDRead() {
    val target = wrapIndex(resource("spark-test-scala-basic-read", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-basic-read", "data", version))
    RestUtils.touch(wrapIndex("spark-test-scala-basic-read"))
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test-scala-basic-read"))

    val esData = EsSpark.esRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.find(_.toString.contains("message")).nonEmpty)

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testEsRDDReadNoType(): Unit = {
    EsAssume.versionOnOrBefore(EsMajorVersion.V_6_X, "after 6.x, it is assumed that new types are unnamed.")
    val doc =
      """{
        |  "id": 1,
        |  "data": [],
        |  "event": "acknowledge"
        |}
      """.stripMargin
    val target = wrapIndex("spark-test-scala-basic-read-notype")
    RestUtils.put(s"$target/events/1", doc.getBytes())
    RestUtils.refresh(target)

    val noType = sc.esRDD(target).first.toString
    val typed = sc.esRDD(s"$target/events").first.toString
    assertEquals(typed, noType)
  }

  @Test
  def testEsRDDReadQuery() {
    val index = "spark-test-scala-basic-query-read"
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index)

    val queryTarget = resource("*-scala-basic-query-read", typename, version)
    val esData = EsSpark.esRDD(sc, queryTarget, "?q=message:Hello World", cfg)
    val newData = EsSpark.esRDD(sc, collection.mutable.Map(cfg.toSeq: _*) += (
      ES_RESOURCE -> queryTarget,
      ES_INPUT_JSON -> "true",
      ES_QUERY -> "?q=message:Hello World"))

    // on each run, 2 docs are added
    assertTrue(esData.count() % 2 == 0)
    assertTrue(newData.count() % 2 == 0)

    assertNotNull(esData.take(10))
    assertNotNull(newData.take(10))
    assertNotNull(esData)
  }

  @Test
  def testEsRDDReadAsJson() {
    val index = wrapIndex("spark-test-scala-basic-json-read")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test-scala-basic-json-read"))

    val esData = EsSpark.esJsonRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.contains("message"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testEsRDDReadWithSourceFilter() {
    val index = wrapIndex("spark-test-scala-source-filter-read")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index)

    val testCfg = cfg + (ConfigurationOptions.ES_READ_SOURCE_FILTER -> "message_date")

    val esData = EsSpark.esRDD(sc, target, testCfg)
    val messages = esData.filter(doc => doc._2.contains("message_date"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testIndexAlias() {
    val doc = """
        | { "number" : 1, "list" : [ "an array", "some value"], "song" : "Golden Eyes" }
        """.stripMargin
    val typename = wrapIndex("type")
    val indexA = wrapIndex("spark-alias-indexa")
    val indexB = wrapIndex("spark-alias-indexb")
    val alias = wrapIndex("spark-alias-alias")
    val aliasTarget = resource(alias, typename, version)
    val docPathA = docEndpoint(indexA, typename, version)
    val docPathB = docEndpoint(indexB, typename, version)

    RestUtils.postData(docPathA + "/1", doc.getBytes())
    RestUtils.postData(docPathB + "/1", doc.getBytes())

    val aliases = """
        |{"actions" : [
          | {"add":{"index":"""".stripMargin + indexA + """" ,"alias": """" + alias + """"}},
          | {"add":{"index":"""".stripMargin + indexB + """" ,"alias": """" + alias + """"}}
        |]}
        """.stripMargin

    println(aliases)
    RestUtils.postData("_aliases", aliases.getBytes())
    RestUtils.refresh(alias)

    val aliasRDD = EsSpark.esJsonRDD(sc, aliasTarget, cfg)
    assertEquals(2, aliasRDD.count())
  }

  @Test
  def testNullAsEmpty() {
    val data = Seq(
      Map("field1" -> 5.4, "field2" -> "foo"),
      Map("field2" -> "bar"),
      Map("field1" -> 0.0, "field2" -> "baz")
    )
    val index = wrapIndex("spark-test-nullasempty")
    val typename = "data"
    val target = resource(index, typename, version)

    sc.makeRDD(data).saveToEs(target)

    assertEquals(3, EsSpark.esRDD(sc, target, cfg).count())
  }

  @Test
  def testNewIndexWithTemplate() {
    val index = wrapIndex("spark-template-index")
    val typename = "alias"
    val target = resource(index, typename, version)

    val indexPattern = "spark-template-*"
    val patternMatchField = if (version.onOrAfter(EsMajorVersion.V_8_X)) {
      s""""index_patterns":["$indexPattern"],"""
    } else {
      s""""template": "$indexPattern","""
    }

    val template = s"""
        |{
        |$patternMatchField
        |"settings" : {
        |    "number_of_shards" : 1,
        |    "number_of_replicas" : 0
        |},
        |"mappings" : ${wrapMapping("alias", s"""{
        |    "properties" : {
        |      "name" : { "type" : "${keyword}" },
        |      "number" : { "type" : "long" },
        |      "@ImportDate" : { "type" : "date" }
        |     }
        |   }}""".stripMargin)},
        |"aliases" : { "spark-temp-index" : {} }
        |}""".stripMargin
    RestUtils.put("_template/" + wrapIndex("test_template"), template.getBytes)

    val rdd = readAsRDD(AbstractScalaEsScalaSpark.testData.sampleArtistsJsonUri())
    EsSpark.saveJsonToEs(rdd, target)
    val esRDD = EsSpark.esRDD(sc, target, cfg)
    println(esRDD.count)
    println(RestUtils.getMappings(index).getResolvedView)

    val erc = new ExtendedRestClient()
    erc.delete("_template/" + wrapIndex("test_template"))
    erc.close()
  }


  @Test
  def testEsSparkVsScCount() {
    val index = wrapIndex("spark-test-check-counting")
    val typename = "data"
    val target = resource(index, typename, version)

    val rawCore = List( Map("colint" -> 1, "colstr" -> "s"),
                         Map("colint" -> 9, "colstr" -> null) )
    sc.parallelize(rawCore, 1).saveToEs(target)
    val qjson =
      """{"query":{"range":{"colint":{"lte":"9"}}}}"""

    val esRDD = EsSpark.esRDD(sc, target, qjson)
    val scRDD = sc.esRDD(target, qjson)
    assertEquals(esRDD.collect().size, scRDD.collect().size)
  }


  @Test
  def testaaaaaMultiIndexNonExisting() {

    val multipleMissingIndicesWithSetting = EsSpark.esJsonRDD(sc, "bumpA,Stump", Map(ES_INDEX_READ_MISSING_AS_EMPTY -> "yes"))
    assertEquals(0, multipleMissingIndicesWithSetting.count)
    val multipleMissingIndicesWithoutSetting = EsSpark.esJsonRDD(sc, "bumpA,Stump")
    try {
      val count = multipleMissingIndicesWithoutSetting.count
      fail("Should have thrown an exception instead of returning " + count)
    } catch {
      case e: EsHadoopIllegalArgumentException => //Expected
    }

    val index1 = prefix + "spark-test-read-missing-as-empty-1"
    val typename = "data"
    val target1 = resource(index1, typename, version)
    val docPath1 = docEndpoint(index1, typename, version)

    RestUtils.touch(target1)
    RestUtils.postData(docPath1, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath1, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index1)

    val index2 = prefix + "spark-test-read-missing-as-empty-2"
    val target2 = resource(index2, typename, version)
    val docPath2 = docEndpoint(index2, typename, version)

    RestUtils.touch(target2)
    RestUtils.postData(docPath2, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath2, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index2)

    val mixedWithSetting = EsSpark.esJsonRDD(sc, "bumpA,Stump," + index1 + "," + index2, Map(ES_INDEX_READ_MISSING_AS_EMPTY -> "yes"))
    assertEquals(4, mixedWithSetting.count)
    val mixedWithoutSetting = EsSpark.esJsonRDD(sc, "bumpA,Stump," + index1)
    try {
      val count = mixedWithoutSetting.count
      fail("Should have thrown an exception instead of returning " + count)
    } catch {
      case e: EsHadoopIllegalArgumentException => //Expected
    }
  }

  @Test
  def testSparkAccumulators() = {
    def config(metricsPrefix: String): Map[String, String] = {
      val index = wrapIndex(s"spark-test-accumulators-${metricsPrefix}")
      val typename = "data"
      val target = resource(index, typename, version)
      Map[String, String](
        ES_RESOURCE_READ -> target,
        ES_RESOURCE_WRITE -> target,
        ES_METRICS_PREFIX -> metricsPrefix
      )
    }

    val data = Seq(
      Map("field1" -> "doc1"),
      Map("field1" -> "doc2"),
      Map("field1" -> "doc3")
    )
    val inputRdd = sc.makeRDD(data)

    inputRdd.saveToEs(config("single-write"))

    val doubleWritesConfig = config("double-write")
    inputRdd.saveToEs(doubleWritesConfig)
    inputRdd.saveToEs(doubleWritesConfig)

    val readWriteConfig = config("read-write")
    inputRdd.saveToEs(readWriteConfig)
    val outputRdd = sc.esRDD(readWriteConfig)

    import org.elasticsearch.hadoop.mr.Counter._
    assertEquals(inputRdd.count(), EsSparkAccumulators.get("single-write", DOCS_SENT))
    assertEquals(inputRdd.count() * 2, EsSparkAccumulators.get("double-write", DOCS_SENT))
    assertEquals(inputRdd.count(), EsSparkAccumulators.get("read-write", DOCS_SENT))
    assertEquals(0, EsSparkAccumulators.get("read-write", DOCS_RECEIVED)) // lazy reads before the action count
    assertEquals(outputRdd.count(), EsSparkAccumulators.get("read-write", DOCS_RECEIVED))
    assertEquals(outputRdd.count() * 2, EsSparkAccumulators.get("read-write", DOCS_RECEIVED))
    EsSparkAccumulators.clear()
  }

  def wrapIndex(index: String) = {
    prefix + index
  }

  def wrapMapping(mappingName: String, mapping: String): String = {
    if (TestUtils.isTypelessVersion(version)) {
      mapping
    } else {
      s"""{"$mappingName":$mapping}"""
    }
  }
}