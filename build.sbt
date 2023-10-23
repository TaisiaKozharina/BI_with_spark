ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.12.61",
  "com.amazonaws" % "aws-java-sdk-logs" % "1.12.61",
  "com.amazonaws" % "aws-java-sdk-core" % "1.12.61"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"


lazy val root = (project in file("."))
  .settings(
    name := "BI_spark"
  )
