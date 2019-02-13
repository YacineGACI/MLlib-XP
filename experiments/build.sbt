name := "experiments"

organization := "fr.insa.distml"

version := "0.1"

scalaVersion := "2.11.12"

scapegoatVersion in ThisBuild := "1.1.0"

// wartremoverWarnings ++= Warts.all

val        sparkVersion = "2.4.0"
val        log4jVersion = "2.11.1"
val        scoptVersion = "3.7.1"
val       hadoopVersion = "2.7.3"
val       circleVersion = "0.8.0"
val sparkMeasureVersion = "0.13"

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"           %        sparkVersion % Provided,
  "org.apache.spark"        %% "spark-sql"            %        sparkVersion % Provided,
  "org.apache.spark"        %% "spark-mllib"          %        sparkVersion % Provided,
  "org.apache.hadoop"        % "hadoop-client"        %       hadoopVersion % Provided,
  "org.apache.logging.log4j" % "log4j-api"            %        log4jVersion % Provided,
  "org.apache.logging.log4j" % "log4j-core"           %        log4jVersion % Provided,
  "com.github.scopt"        %% "scopt"                %        scoptVersion,
  "io.circe"                %% "circe-yaml"           %       circleVersion,
  "io.circe"                %% "circe-generic"        %       circleVersion,
  "io.circe"                %% "circe-generic-extras" %       circleVersion,
  "ch.cern.sparkmeasure"    %% "spark-measure"        % sparkMeasureVersion
)

excludeDependencies ++= Seq(
  ExclusionRule(organization = "org.glassfish.hk2.external")
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case "overview.html"                    => MergeStrategy.rename
  case "git.properties"                   => MergeStrategy.last
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}
