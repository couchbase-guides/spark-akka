package com.knoldus.couchbaseServices.routes

import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.couchbase.client.java.document.json.JsonObject
import com.knoldus.couchbaseServices.factories.DatabaseAccess


trait SparkService extends DatabaseAccess {

  implicit val system:ActorSystem
  implicit val materializer:ActorMaterializer
  val logger = Logging(system, getClass)

  implicit def myExceptionHandler =
    ExceptionHandler {
      case e: ArithmeticException =>
        extractUri { uri =>
          complete(HttpResponse(StatusCodes.InternalServerError, entity = s"Data is not persisted and something went wrong"))
        }
    }

  val sparkRoutes: Route = {
    get {
      path("insert" / "name" / Segment / "email" / Segment) { (name: String, email: String) =>
        complete {
          val documentId = "user::" + UUID.randomUUID().toString
          try {
            val jsonObject = JsonObject.create().put("name", name).put("email", email)
            val isPersisted = persistOrUpdate(documentId, jsonObject)
            isPersisted match {
              case true => HttpResponse(StatusCodes.Created, entity = s"Data is successfully persisted with id $documentId")
              case false => HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for id : $documentId")
            }
          } catch {
            case ex: Throwable =>
              logger.error(ex, ex.getMessage)
              HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for id : $documentId")
          }
        }
      }
    } ~ path("updateViaKV" / "name" / Segment / "email" / Segment / "id" / Segment) { (name: String, email: String, id: String) =>
      get {
        complete {
          try {
            val documentId = id
            val jsonObject = JsonObject.create().put("name", name).put("email", email)
            val isPersisted = persistOrUpdate(documentId, jsonObject)
            isPersisted match {
              case true => HttpResponse(StatusCodes.Created, entity = s"Data is successfully persisted with id $documentId")
              case false => HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for id : $documentId")
            }
          } catch {
            case ex: Throwable =>
              logger.error(ex, ex.getMessage)
              HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for id : $id")
          }
        }
      }
    } ~ path("getViaKV" / "id" / Segment) { (listOfIds: String) =>
      get {
        complete {
          try {
            val idAsRDD: Option[Array[String]] = getViaKV(listOfIds)
            idAsRDD match {
              case Some(data) => HttpResponse(StatusCodes.OK, entity = data.mkString(","))
              case None => HttpResponse(StatusCodes.InternalServerError, entity = s"Data is not fetched and something went wrong")
            }
          } catch {
            case ex: Throwable =>
              logger.error(ex, ex.getMessage)
              HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for ids : $listOfIds")
          }
        }
      }
    } ~
      path("getViaView" / "name" / Segment) { (name: String) =>
        get {
          complete {
            val emailFetched: Option[Array[String]] = getViaView(name)
            emailFetched match {
              case Some(data) => HttpResponse(StatusCodes.OK, entity = data.mkString(","))
              case None => HttpResponse(StatusCodes.InternalServerError, entity = s"Data is not fetched and something went wrong")
            }
          }
        }
      } ~
      path("getViaN1Ql" / "name" / Segment) { (name: String) =>
        get {
          complete {
            val emailFetched = getViaN1Ql(name)
            emailFetched match {
              case Some(data) => HttpResponse(StatusCodes.OK, entity = data.mkString(","))
              case None => HttpResponse(StatusCodes.InternalServerError, entity = s"Data is not fetched and something went wrong")
            }
          }
        }
      } ~ path("delete" / "id" / Segment) { (id: String) =>
      get {
        complete {
          try {
            val idAsRDD: Option[Array[String]] = deleteViaId(id)
            idAsRDD match {
              case Some(data) => HttpResponse(StatusCodes.OK, entity = data.mkString(",") + "is deleted")
              case None => HttpResponse(StatusCodes.InternalServerError, entity = s"Data is not fetched and something went wrong")
            }
          } catch {
            case ex: Throwable =>
              logger.error(ex, ex.getMessage)
              HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for ids : $id")
          }
        }
      }
    }
  }
}
