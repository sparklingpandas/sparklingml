organization := "com.sparklingpandas"

name := "sparklingml"

publishMavenStyle := true

version := "0.0.1"

sparkVersion := "2.1.1"

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



javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= Seq(
  "com.lucidwords.spark" % "spark-solr" % "3.0.2")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalacheck" %% "scalacheck" % "1.13.4",
  "com.holdenkarau" %% "spark-testing-base_2.1.1" % "0.7.0")


scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

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

credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"), Credentials(Path.userHome / ".ivy2" / ".sparkcredentials"))

spIncludeMaven := true

useGpg := true
