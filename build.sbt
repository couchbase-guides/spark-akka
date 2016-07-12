
name := "spark-couchbase-akka-http-starter-kit"

version := "1.0"

scalaVersion := "2.11.7"

organization := "com.knoldus"

val akkaV = "2.4.5"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  "com.typesafe.akka" %% "akka-http-core" % akkaV,
  "com.typesafe.akka" %% "akka-http-experimental" % akkaV,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaV % "test",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaV,
  "org.scalatest"     %% "scalatest" % "2.2.6" % "test",
  "com.couchbase.client" %% "spark-connector" % "1.1.0"
)

assembleArtifact in packageScala := false // We don't need the Scala library, Spark already includes it

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
fork in run := true
