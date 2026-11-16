package com.bank.sa.kafka

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.engine.ExplainableCreditEngine
import com.bank.sa.api.LoanOriginationServer
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.{Cluster, Join}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

/**
 * Standalone CLI application for generating thousands of transactions (including Hot-Partition customers)
 * into Kafka (`sa.transactions.raw.v1`) and observing automatic processing and LLM situation summarization.
 *
 * Usage:
 *   sbt "project customerAgents" "runMain com.bank.sa.kafka.KafkaTransactionGeneratorApp 3000"
 */
object KafkaTransactionGeneratorApp:

  def main(args: Array[String]): Unit =
    val totalTx  = args.headOption.flatMap(_.toIntOption).getOrElse(2500)
    val hotShare = args.lift(1).flatMap(_.toDoubleOption).getOrElse(0.65)

    val config = ConfigFactory.load()
    val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "BankingSituationCluster", config)
    given ActorSystem[Nothing] = system
    given ExecutionContext     = system.executionContext

    try
      val cluster = Cluster(system)
      cluster.manager ! Join(cluster.selfMember.address)

      val creditEngine = new ExplainableCreditEngine()
      val gateway = new CustomerClusterGateway(
        system = system,
        creditEngine = creditEngine,
        llmTxCountThreshold = 250
      )
      val pipeline = new KafkaTransactionPipeline(gateway, () => LoanOriginationServer.defaultWatchlist)

      println("================================================================================")
      println(s" KAFKA TRANSACTION GENERATOR (Hot-Partition & AI-Models LLM Summary Demo)")
      println(s" Generating $totalTx transactions (Hot-Partition Traffic Share: ${(hotShare * 100).toInt}%)")
      println("================================================================================")

      val req = KafkaGeneratorRequest(
        totalTransactions = totalTx,
        normalCustomersCount = 60,
        hotPartitionCustomerIds = List("CUST-HOT-MEGA-01", "CUST-HOT-WHALE-02", "CUST-ALICE-001"),
        hotPartitionTrafficShare = hotShare,
        highRiskRatio = 0.08
      )

      val batchRes = pipeline.generateAndPublishTransactions(req)
      println(s"[1] Published Batch '${batchRes.batchId}' to topic '${batchRes.topic}' (${batchRes.brokerMode}) in ${batchRes.generationTimeMs}ms:")
      println(s"    - Hot-Partition Transactions : ${batchRes.hotPartitionTransactionsCount}")
      println(s"    - Normal Transactions        : ${batchRes.normalTransactionsCount}")
      println(s"    - Hot Customer Breakdown     : ${batchRes.hotCustomerCounts.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
      println(s"    - Top Kafka Partitions       : ${batchRes.partitionCounts.toSeq.sortBy(-_._2).take(5).map { case (p, c) => s"P$p->$c txs" }.mkString(", ")}")

      while pipeline.pendingQueueLag > 0 do
        Await.result(pipeline.drainAvailableTransactions(1000), 15.seconds)

      val (hotState, _) = Await.result(
        gateway.getOrGenerateCustomerSituationDescription("CUST-HOT-MEGA-01", forceRefresh = true, "CLI_VERIFICATION"),
        10.seconds
      )

      println("--------------------------------------------------------------------------------")
      println(s"[2] Hot-Partition CustomerAgent('CUST-HOT-MEGA-01') Situation State:")
      println(s"    - State Version              : v${hotState.stateVersion}")
      println(s"    - Total Txs Processed        : ${hotState.totalTransactionsProcessed}")
      println(s"    - Ego-Network Peers (Bounded): ${hotState.egoNetwork.size} (Super-Node=${hotState.isHighDegreeSuperNode})")
      println(s"    - LLM Summaries Generated    : ${hotState.llmSummariesCount}")
      println(s"    - Latest LLM Narrative       :\n      ${hotState.lastLlmSummary.map(_.narrativeDescription).getOrElse("N/A")}")
      println("================================================================================")

      pipeline.close()
      gateway.llmSummarizer match
        case svc: com.bank.sa.llm.OpenAiLlmService => svc.close()
        case _                                     => ()
    finally
      system.terminate()
      Await.result(system.whenTerminated, 10.seconds)
