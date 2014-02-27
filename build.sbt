import scalariform.formatter.preferences._

scalariformSettings

name := "akka-memcached"

version := "0.91"

scalaVersion := "2.10.0"

libraryDependencies ++= Seq(
    "log4j" % "log4j" % "1.2.14",
    "jboss" % "jboss-serialization" % "1.0.3.GA",
    "com.typesafe.akka" % "akka-actor" % "2.0.3",
    "com.google.guava" % "guava" % "13.0"
)

resolvers ++= Seq(
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

organization := "com.klout"

scalacOptions in Compile ++= Seq("-deprecation", "-unchecked")

publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local")

credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil

scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences().
    setPreference(AlignParameters, true).
    setPreference(IndentSpaces, 4).
    setPreference(AlignSingleLineCaseStatements, true).
    setPreference(PreserveDanglingCloseParenthesis, true).
    setPreference(PreserveSpaceBeforeArguments, true)
)
