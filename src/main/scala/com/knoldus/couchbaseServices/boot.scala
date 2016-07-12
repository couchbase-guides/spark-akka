package com.knoldus.couchbaseServices

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.knoldus.couchbaseServices.routes.SparkService


class StartSparkServer(implicit val system: ActorSystem,
                       implicit val materializer: ActorMaterializer) extends SparkService {
  def startServer(address: String, port: Int) = {
    Http().bindAndHandle(sparkRoutes, address, port)
  }
}

object StartApplication extends App {
  StartApp
}

object StartApp {
  implicit val system: ActorSystem = ActorSystem("Spark-Couchbase-Service")
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()
  val server = new StartSparkServer()
  val config = server.config
  val serverUrl = config.getString("http.interface")
  val port = config.getInt("http.port")
  server.startServer(serverUrl, port)
}