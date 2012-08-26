import com.typesafe.sbtscalariform.ScalariformPlugin._
import scalariform.formatter.preferences._

name := "akka-memcached"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 
libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.3"

scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences().
    setPreference(AlignParameters, true).
    setPreference(IndentSpaces, 4).
    setPreference(AlignSingleLineCaseStatements, true).
    setPreference(PreserveDanglingCloseParenthesis, true).
    setPreference(PreserveSpaceBeforeArguments, true)
)
