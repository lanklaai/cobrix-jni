ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.cobrixjni"

lazy val root = (project in file("."))
  .settings(
    name := "cobrix-jni-bridge",
    libraryDependencies ++= Seq(
      "za.co.absa.cobrix" %% "cobol-parser" % "2.10.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2"
    )
  )
