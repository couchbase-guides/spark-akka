

package com.knoldus.couchbaseServices

import java.util.UUID

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.couchbase.client.java.document.json.JsonObject
import com.knoldus.couchbaseServices.routes.SparkService
import org.scalatest.{Matchers, WordSpec}

class SparkServicesSpec extends WordSpec with Matchers with ScalatestRouteTest with SparkService {

  val documentId = "user::" + UUID.randomUUID().toString
  val jsonObject = JsonObject.create().put("name", "Shivansh").put("email", "shivansh@knoldus.com")
  val jsonDocument = persistOrUpdate(documentId, jsonObject)
  "The service" should {

    "be able to insert data in the couchbase" in {
      Get("/insert/name/Shivansh/email/shiv4nsh@gmail.com") ~> sparkRoutes ~> check {
        responseAs[String].contains("Data is successfully persisted with id") shouldEqual true
      }
    }

    "to be able to retrieve data via N1Ql" in {
      Get("/getViaN1Ql/name/Shivansh") ~> sparkRoutes ~> check {
        responseAs[String].contains("shivansh@knoldus.com") shouldEqual true
      }
    }
    "be able to retrieve data via View query" in {
      Get("/getViaView/name/Shivansh") ~> sparkRoutes ~> check {
        responseAs[String].contains("shivansh@knoldus.com") shouldEqual true
      }
    }

    "be able to retrieve data via KV operation" in {
      Get(s"/getViaKV/id/$documentId") ~> sparkRoutes ~> check {
        responseAs[String].contains("shivansh@knoldus.com") shouldEqual true
      }
    }
    "be able to update data via KV operation" in {
      Get(s"/updateViaKV/name/Shivansh/email/shivansh@knoldus.com/id/$documentId") ~> sparkRoutes ~> check {
        responseAs[String].contains("Data is successfully persisted with id") shouldEqual true
      }
    }
  }
}
