import sbt._
import Keys._

val scioVersion = "0.12.7"
val beamVersion = "2.42.0"
val scalaMajorVersion = "2.13"
val scalaMinorVersion = "7"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.spotify.pipeline",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := s"$scalaMajorVersion.$scalaMinorVersion",
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val root: Project = Project(
  "spotify-data-pipeline",
  file(".")
).settings(
  commonSettings,
  description := "Spotify Data Pipeline using Scio",
  libraryDependencies ++= Seq(
    // Scio Core
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % Test,
    "com.spotify" %% "scio-jdbc" % scioVersion,
    "com.spotify" %% "scio-parquet" % scioVersion,
    
    // Apache Beam
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "org.apache.beam" % "beam-sdks-java-io-hadoop-file-system" % beamVersion,
    "org.apache.beam" % "beam-sdks-java-io-parquet" % beamVersion,
    
    // Azure Data Lake Storage support
    "org.apache.hadoop" % "hadoop-azure" % "3.3.6",
    "org.apache.hadoop" % "hadoop-client" % "3.3.6",
    "com.microsoft.azure" % "azure-storage" % "8.6.6",
    "com.azure" % "azure-storage-file-datalake" % "12.15.0",
    
    // Database connectors
    "org.postgresql" % "postgresql" % "42.6.0",
    "com.databricks" % "databricks-jdbc" % "2.6.29",
    
    // Configuration and utilities
    "com.typesafe" % "config" % "1.4.2",
    "com.github.pureconfig" %% "pureconfig" % "0.17.4",
    
    // Logging
    "org.slf4j" % "slf4j-api" % "2.0.7",
    "ch.qos.logback" % "logback-classic" % "1.4.8",
    
    // Testing
    "org.scalatest" %% "scalatest" % "3.2.16" % Test
  ),
  
  // Assembly settings
  assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case x => MergeStrategy.first
  }
)

// Add assembly plugin
addCommandAlias("runStreamingHistoryTransform", "runMain com.spotify.pipeline.transforms.StreamingHistoryTransform") 