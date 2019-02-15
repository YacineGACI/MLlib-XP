name := "experiment"

organization := "fr.insa.distml"

version := "0.1"

scalaVersion := "2.11.12"

scapegoatVersion := "1.1.0"

// wartremoverWarnings ++= Warts.all

val        sparkVersion = "2.4.0"
val        scoptVersion = "3.7.1"
val         playVersion = "2.4.8"
val sparkMeasureVersion = "0.13"

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"           %        sparkVersion % Provided,
  "org.apache.spark"        %% "spark-sql"            %        sparkVersion % Provided,
  "org.apache.spark"        %% "spark-mllib"          %        sparkVersion % Provided,
  "com.github.scopt"        %% "scopt"                %        scoptVersion,
  "com.typesafe.play"       %% "play-json"            %         playVersion,
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
