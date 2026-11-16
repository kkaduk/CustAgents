ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "com.bank.sa"
ThisBuild / resolvers += Resolver.mavenLocal
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

val pekkoVersion     = "1.1.2"
val pekkoHttpVersion = "1.1.0"
val flinkVersion     = "1.19.1"
val ojdbcVersion     = "23.6.0.24.10"
val scalatestVersion = "3.2.19"
val logbackVersion   = "1.5.12"
val openAiScalaClientVersion = "1.6.0"
val jacksonVersion   = "2.17.2"

ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"     % "jackson-core"                   % jacksonVersion,
  "com.fasterxml.jackson.core"     % "jackson-annotations"            % jacksonVersion,
  "com.fasterxml.jackson.core"     % "jackson-databind"               % jacksonVersion,
  "com.fasterxml.jackson.module"  %% "jackson-module-scala"           % jacksonVersion,
  "com.fasterxml.jackson.module"   % "jackson-module-parameter-names" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310"        % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8"          % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"      % jacksonVersion
)

lazy val root = (project in file("."))
  .aggregate(domain, explainableEngine, enrichmentPipeline, aiModels, customerAgents)
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
  .dependsOn(domain, explainableEngine, enrichmentPipeline, aiModels)
  .settings(
    name := "sa-customer-agents",
    Compile / mainClass := Some("com.bank.sa.app.SituationAwarenessDemo"),
    libraryDependencies ++= Seq(
      "org.apache.pekko"         %% "pekko-actor-typed"            % pekkoVersion,
      "org.apache.pekko"         %% "pekko-stream"                 % pekkoVersion,
      "org.apache.pekko"         %% "pekko-cluster-typed"          % pekkoVersion,
      "org.apache.pekko"         %% "pekko-cluster-sharding-typed" % pekkoVersion,
      "org.apache.pekko"         %% "pekko-persistence-typed"      % pekkoVersion,
      "org.apache.pekko"         %% "pekko-serialization-jackson"  % pekkoVersion,
      "org.apache.pekko"         %% "pekko-http"                   % pekkoHttpVersion,
      "org.apache.pekko"         %% "pekko-slf4j"                  % pekkoVersion,
      "org.apache.kafka"          % "kafka-clients"                % "3.8.0",
      "com.oracle.database.jdbc"  % "ojdbc11"                      % ojdbcVersion,
      "ch.qos.logback"            % "logback-classic"              % logbackVersion,
      "org.apache.pekko"         %% "pekko-actor-testkit-typed"    % pekkoVersion     % Test,
      "org.apache.pekko"         %% "pekko-stream-testkit"         % pekkoVersion     % Test,
      "org.apache.pekko"         %% "pekko-http-testkit"           % pekkoHttpVersion % Test,
      "org.apache.pekko"         %% "pekko-persistence-testkit"    % pekkoVersion     % Test,
      "org.scalatest"            %% "scalatest"                    % scalatestVersion % Test
    )
  )

lazy val aiModels = (project in file("modules/ai-models"))
  .dependsOn(domain)
  .settings(
    name := "sa-ai-models",
    libraryDependencies ++= Seq(
      "io.cequence"      %% "openai-scala-client"          % openAiScalaClientVersion,
      "org.apache.pekko" %% "pekko-actor-typed"            % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"                 % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"                   % pekkoHttpVersion,
      "ch.qos.logback"    % "logback-classic"              % logbackVersion,
      "org.scalatest"    %% "scalatest"                    % scalatestVersion % Test
    )
  )