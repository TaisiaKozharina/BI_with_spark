ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

//libraryDependencies += "org.vegas-viz" %% "vegas-spark" % "{vegas-version}" -- MIGHT USE LATER FOR VISUALS

lazy val root = (project in file("."))
  .settings(
    name := "BI_spark"
  )
