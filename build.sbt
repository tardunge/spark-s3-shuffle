//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache2.0
//

scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.13.12")
organization := "com.ibm"
name := "spark-s3-shuffle"
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "4.0.0")

enablePlugins(BuildInfoPlugin)
// GitVersioning disabled to use explicit version
// enablePlugins(GitVersioning)

// Git
git.useGitDescribe := true
git.uncommittedSignifier := Some("DIRTY")

// Build info
buildInfoObject := "SparkS3ShuffleBuild"
buildInfoPackage := "com.ibm"
buildInfoKeys ++= Seq[BuildInfoKey](
  BuildInfoKey.action("buildTime") {
    System.currentTimeMillis
  },
  BuildInfoKey.action("sparkVersion") {
    sparkVersion
  }
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion % "compile",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M")
scalacOptions ++= Seq("-deprecation", "-unchecked")

// Java 11+ module system flags for Kryo serialization in tests
Test / javaOptions ++= Seq(
  "--add-opens", "java.base/java.lang=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens", "java.base/java.io=ALL-UNNAMED",
  "--add-opens", "java.base/java.net=ALL-UNNAMED",
  "--add-opens", "java.base/java.nio=ALL-UNNAMED",
  "--add-opens", "java.base/java.util=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED"
)
Test / fork := true

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision +
    artifact.classifier.fold("")("-" + _) + "." + artifact.extension
}

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}-with-dependencies.jar"
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

lazy val lib = (project in file("."))
