
name := "ShazamEngine"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"     % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql"      % "2.1.0" % "provided",
  "com.databricks" % "spark-csv_2.11" % "1.5.0"
)

mainClass in assembly := Some("com.ctm.Main")

testOptions in Test := Seq(Tests.Filter(name => name.toLowerCase().contains("runtests")))

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.amazonaws.**" -> "ctm.com.amazonaws.@1").inAll,
  ShadeRule.rename("org.apache.http.**" -> "ctm.org.apache.http.@1").inAll,
  ShadeRule.rename("org.joda.time.**" -> "ctm.org.joda.time.@1").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "ctm.com.fasterxml.jackson.@1").inAll,
  ShadeRule.rename("org.apache.commons.codec.**" -> "ctm.org.apache.commons.codec.@1").inAll,
  ShadeRule.rename("org.apache.commons.logging.**" -> "ctm.org.apache.commons.logging.@1").inAll,
  ShadeRule.rename("org.json4s.**" -> "ctm.org.json4s.@1").inAll
)


val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}


assemblyJarName in assembly := name.value + ".jar"
