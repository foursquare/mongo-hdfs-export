name := "mongo-hdfs-export"

version := "0.0.1"

organization := "com.foursquare"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop"      % "hadoop-client"      % "2.0.0-mr1-cdh4.4.0",
  "org.apache.hadoop"      % "hadoop-core"        % "2.0.0-mr1-cdh4.4.0",
  "org.mongodb"            % "mongo-java-driver"  % "2.11.3",
  "org.scalaj"             %% "scalaj-collection" % "1.5",
  "com.hadoop.gplcompression" % "hadoop-lzo"         % "0.4.15",
  "org.slf4j"              % "slf4j-api"          % "1.6.4",
  "org.slf4j"              % "jcl-over-slf4j"     % "1.6.4",
  "org.slf4j"              % "jul-to-slf4j"       % "1.6.4",
  "org.slf4j"              % "log4j-over-slf4j"   % "1.6.4"
)

crossScalaVersions := Seq("2.9.2", "2.10.3")

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

resolvers += "twttr.com" at "http://maven.twttr.com/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/public/"

mainClass in (Compile, run) := Some("com.foursquare.hadoop.export.MongoDump")
