name := "graphx-rosen"

organization := ""

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-graphx" % "1.3.1" % "provided" withSources() withJavadoc()
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

