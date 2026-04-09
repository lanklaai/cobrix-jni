import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.cobrixjni"

lazy val root = (project in file("."))
  .settings(
    name := "cobrix-jni-bridge",
    libraryDependencies ++= Seq(
      "za.co.absa.cobrix" %% "cobol-parser" % "2.10.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2"
    ),
    assembly / mainClass := None,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.endsWith(".sf")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.endsWith(".dsa")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.endsWith(".rsa")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase == "index.list") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase == "dependencies") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
