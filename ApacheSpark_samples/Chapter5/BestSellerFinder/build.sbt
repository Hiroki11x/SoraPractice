name := "BestSellerFinder"
version := "0.1"
scalaVersion := "2.10.4"
libraryDependencies ++=
Seq("org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
