package com.bank.sa.app

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.api.LoanOriginationServer
import com.bank.sa.engine.ExplainableCreditEngine
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.{Cluster, Join}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

/**
 * Starts the Pekko Cluster Sharding node and binds the Pekko HTTP / gRPC server on port 8080,
 * serving both the Interactive Business UI (`http://127.0.0.1:8080/`) and the REST/gRPC API.
 */
object LoanOriginationWebApp:

  def main(args: Array[String]): Unit =
    val system = ActorSystem[Nothing](Behaviors.empty, "BankingSituationAwarenessCluster")
    given ActorSystem[Nothing] = system
    given ExecutionContext     = system.executionContext

    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)

    val creditEngine = new ExplainableCreditEngine()
    val gateway      = new CustomerClusterGateway(system, creditEngine)
    val server       = new LoanOriginationServer(gateway)

    val host    = sys.env.getOrElse("SA_HTTP_HOST", "127.0.0.1")
    val port    = sys.env.getOrElse("SA_HTTP_PORT", "8088").toInt
    val binding = Await.result(server.start(host, port), 15.seconds)

    println("=" * 88)
    println(s" Banking Situation Awareness Workbench & API running at: http://$host:${binding.localAddress.getPort}/")
    println(s" REST API Base : http://$host:${binding.localAddress.getPort}/api/v1/system/overview")
    println(s" gRPC Endpoint : http://$host:${binding.localAddress.getPort}/com.bank.sa.proto.v1.LoanOriginationService/EvaluateLoan")
    println("=" * 88)

    Await.result(system.whenTerminated, Duration.Inf)
