
name := "FlinkStreamingExample"

version := "0.1"

scalaVersion in ThisBuild := "2.11.11"

autoScalaLibrary := false

val flinkVersion = "1.3.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion
  )
  
val HbaseDependencies = Seq(
	"org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.12.1",
	"org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.12.1",
	"org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.12.1"
)
lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
	libraryDependencies ++= HbaseDependencies
  )


resolvers ++= Seq(
"MavenRepository" at "https://mvnrepository.com/",
"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
   oldStrategy(x)
}

