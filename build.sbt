import com.typesafe.sbtscalariform.ScalariformPlugin._
import scalariform.formatter.preferences._

name := "akka-memcached"

version := "0.1"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
 	"log4j" % "log4j" % "1.2.14",
	"jboss" % "jboss-serialization" % "1.0.3.GA",
	"trove" % "trove" % "1.0.2",
	"com.typesafe.akka" % "akka-actor" % "2.0.3",
  	"org.specs2" %% "specs2" % "1.12.1" % "test"
)

resolvers ++= Seq(	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
					"snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                    "releases"  at "http://oss.sonatype.org/content/repositories/releases")

scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences().
    setPreference(AlignParameters, true).
    setPreference(IndentSpaces, 4).
    setPreference(AlignSingleLineCaseStatements, true).
    setPreference(PreserveDanglingCloseParenthesis, true).
    setPreference(PreserveSpaceBeforeArguments, true)
)

