// Copyright (c) 2016, 2020 Oracle and/or its affiliates.
name := "csv_to_parquet"
version := "1.0"
scalaVersion := "2.13.12"

val sparkVersion = "3.5.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql-api" % "3.5.0",
  "org.scala-lang" % "scala-compiler" % "2.13.2",
  "org.scala-tools" % "maven-scala-plugin" % "2.14"
 // "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.1-SNAPSHOT" from ("file://Users/siselvan/Documents/spark-3.5.1-SNAPSHOT-bin-3.3.4/jars/spark-connect-client-jvm_2.13-3.5.1-SNAPSHOT.jar"),
)