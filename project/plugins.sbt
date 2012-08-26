 // Add repos
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
       
resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"
              
// Scalariform to autoformat our code
addSbtPlugin("com.typesafe.sbtscalariform" % "sbtscalariform" % "0.3.1")
