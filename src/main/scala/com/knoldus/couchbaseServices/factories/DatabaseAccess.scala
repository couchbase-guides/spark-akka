package com.knoldus.couchbaseServices.factories

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark._
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by shivansh on 9/5/16.
  */
trait DatabaseAccess {

  val config = ConfigFactory.load("application.conf")
  val couchbaseUrl = config.getString("couchbase.url")
  val bucketName = config.getString("couchbase.bucketName")

  val bucketPassword = config.getString("couchbase.bucketPassword")

  val sparkConf: SparkConf = new SparkConf().setAppName("spark-akka-http-couchbase-starter-kit").setMaster("local")
    .set("com.couchbase.nodes", couchbaseUrl).set(s"com.couchbase.bucket.$bucketName", bucketPassword)
  val sc = new SparkContext(sparkConf)
  val NIQLQUERY = s"SELECT * FROM `$bucketName` WHERE name LIKE"
  val VIEWNAME = "emailtoName"
  val DDOCNAME = "userddoc"

  def getNIQLDeleteQuery(documentId: String) =s"""DELETE FROM $bucketName p USE KEYS "$documentId" RETURNING p"""

  def persistOrUpdate(documentId: String, jsonObject: JsonObject): Boolean = {
    val jsonDocument = JsonDocument.create(documentId, jsonObject)
    val savedData = sc.parallelize(Seq(jsonDocument))
    Try(savedData.saveToCouchbase()).toOption.fold(false)(x => true)
  }

  def getViaN1Ql(name: String): Option[Array[String]] = {
    val n1qlRDD = Try(sc.couchbaseQuery(N1qlQuery.simple(NIQLQUERY + s"'$name%'")).collect()).toOption
    n1qlRDD.map(_.map(a => a.value.toString))
  }

  def getViaView(name: String): Option[Array[String]] = {
    val viewRDDData = Try(sc.couchbaseView(ViewQuery.from(DDOCNAME, VIEWNAME).startKey(name)).collect()).toOption
    viewRDDData.map(_.map(a => a.value.toString))
  }

  def getViaKV(listOfDocumentIds: String): Option[Array[String]] = {
    val idAsRDD = sc.parallelize(listOfDocumentIds.split(","))
    Try(idAsRDD.couchbaseGet[JsonDocument]().map(_.content.toString).collect).toOption
  }

  def deleteViaId(documentID: String): Option[Array[String]] = {
    val n1qlRDD = Try(sc.couchbaseQuery(N1qlQuery.simple(getNIQLDeleteQuery(documentID))).collect()).toOption
    n1qlRDD.map(_.map(a => a.value.toString))
  }
}

object DatabaseAccess extends DatabaseAccess

