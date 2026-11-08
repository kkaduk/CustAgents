ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "com.bank.sa"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

val pekkoVersion     = "1.1.2"
val pekkoHttpVersion = "1.1.0"
val flinkVersion     = "1.19.1"
val scalatestVersion = "3.2.19"
val logbackVersion   = "1.5.12"

lazy val root = (project in file("."))
  .aggregate(domain, explainableEngine, enrichmentPipeline, customerAgents)
  .settings(
    name := "banking-situation-awareness",
    publish / skip := true
  )

lazy val domain = (project in file("modules/domain"))
  .settings(
    name := "sa-domain",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val explainableEngine = (project in file("modules/explainable-engine"))
  .dependsOn(domain)
  .settings(
    name := "sa-explainable-engine",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val enrichmentPipeline = (project in file("modules/enrichment-pipeline"))
  .dependsOn(domain)
  .settings(
    name := "sa-enrichment-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.flink"              % "flink-streaming-java"     % flinkVersion,
      "org.apache.flink"              % "flink-connector-kafka"    % "3.2.0-1.19",
      "com.fasterxml.jackson.module" %% "jackson-module-scala"     % "2.17.2",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.17.2",
      "ch.qos.logback"                % "logback-classic"          % logbackVersion,
      "org.scalatest"                %% "scalatest"                % scalatestVersion % Test
    )
  )

lazy val customerAgents = (project in file("modules/customer-agents"))
  .dependsOn(domain, explainableEngine, enrichmentPipeline)
  .settings(
    name := "sa-customer-agents",
    Compile / mainClass := Some("com.bank.sa.app.SituationAwarenessDemo"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed"            % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"                 % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed"          % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-persistence-typed"      % pekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson"  % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"                   % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-slf4j"                  % pekkoVersion,
      "ch.qos.logback"    % "logback-classic"              % logbackVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed"    % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-stream-testkit"         % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-http-testkit"           % pekkoHttpVersion % Test,
      "org.apache.pekko" %% "pekko-persistence-testkit"    % pekkoVersion     % Test,
      "org.scalatest"    %% "scalatest"                    % scalatestVersion % Test
    )
  )

