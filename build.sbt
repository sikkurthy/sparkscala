ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "MySpark"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"
libraryDependencies += "org.eclipse.jetty" % "jetty-client" % "8.1.14.v20131031"
libraryDependencies += "com.snowflake" % "snowpark" % "1.8.0"
libraryDependencies += "net.snowflake" % "snowflake-ingest-sdk" % "0.10.8"
libraryDependencies += "net.snowflake" % "snowflake-jdbc" % "3.13.3"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "10.2.3.jre11"
libraryDependencies += "com.microsoft.azure" % "spark-mssql-connector" % "1.0.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.13"
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.46"
)
libraryDependencies += "joda-time" % "joda-time" % "2.9.3"
libraryDependencies += "org.joda" % "joda-convert" % "1.8"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "2.8.47" % Test
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.11.1"

