
name := "Tes_Fat_Jar"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0" % "provided"

resolvers in Global ++= Seq(
  "Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server" at "https://repo1.maven.org/maven2",
  "TypeSafe Repository Releases" at "https://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}