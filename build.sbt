organization := "com.sparklingpandas"

name := "sparklingml"

publishMavenStyle := true

version := "0.0.1-SNAPSHOT"

sparkVersion := "2.2.0"

scalaVersion := "2.11.8"

coverageHighlighting := true

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

//tag::spName[]
spName := "sparklingpandas/sparklingml"
//end::spName[]

sparkComponents := Seq("core", "sql", "catalyst", "mllib")

parallelExecution in Test := false
fork := true


coverageHighlighting := true
coverageEnabled := true


javaOptions ++= Seq("-Xms1G", "-Xmx1G", "-XX:MaxPermSize=1024M", "-XX:+CMSClassUnloadingEnabled")

test in assembly := {}

libraryDependencies ++= Seq(
  // spark components
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "provided",
  // algorithm providers
  "org.apache.lucene" % "lucene-analyzers-common" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-icu" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-kuromoji" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-morfologik" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-phonetic" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-smartcn" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-stempel" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-uima" % "6.6.0",
  // internals that are only used during code gen
  // TODO(holden): exclude from assembly but keep for runMain somehow?
  "org.scala-lang" % "scala-reflect" % "2.11.7" % "provided",
  "org.reflections" % "reflections" % "0.9.11" % "provided",
  // testing libraries
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "0.7.3" % "test")


scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public"),
  // restlet has a seperate maven repo because idk
  "restlet" at "http://maven.restlet.com"
)

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" ->
  url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/sparklingpandas/sparklingml"))

pomExtra := (
  <scm>
    <url>git@github.com:sparklingpandas/sparklingml.git</url>
    <connection>scm:git@github.com:sparklingpandas/sparklingml.git</connection>
  </scm>
  <developers>
    <developer>
      <id>holdenk</id>
      <name>Holden Karau</name>
      <url>http://www.holdenkarau.com</url>
      <email>holden@pigscanfly.ca</email>
    </developer>
  </developers>
)

credentials ++= Seq(
  Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"),
  Credentials(Path.userHome / ".ivy2" / ".sparkcredentials"))

spIncludeMaven := true

useGpg := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
