name := "CmpDir"

version := "1.0.0"

scalaVersion := "2.11.7"

resolvers ++=
  Seq("Akka Repository" at "http://repo.akka.io/releases/",
      "Typesafe Snapshots" at "http://repo.akka.io/snapshots/",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "maven repo" at "http://central.maven.org/maven2"
  )

libraryDependencies ++=
  Seq(
    //"com.typesafe.akka" % "akka-actor_2.1.1" % "2.3.12",
    "com.typesafe.akka" % "akka-actor_2.11" % "2.3.12",
    "com.typesafe" % "config" % "1.3.0"
  )
