package com.bank.sa.app

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import com.bank.sa.enrichment.EnrichmentLogic
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.{Cluster, Join}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

object SituationAwarenessDemo:

  def main(args: Array[String]): Unit =
    val system = ActorSystem[Nothing](Behaviors.empty, "BankingSituationAwarenessCluster")
    given ExecutionContext = system.executionContext

    try
      // Form a single-node Pekko Cluster for the local simulation
      val cluster = Cluster(system)
      cluster.manager ! Join(cluster.selfMember.address)

      val creditEngine = new ExplainableCreditEngine()
      val gateway      = new CustomerClusterGateway(system, creditEngine)

      val now = Instant.parse("2026-11-07T18:00:00Z")

      println("=" * 96)
      println(" BANKING SITUATION AWARENESS: APACHE PEKKO CLUSTER + ORACLE EXADATA (SCALA 3)")
      println(" Hierarchical Model: Customer -> Relationship -> Portfolio -> Global Banking Situation")
      println("=" * 96)

      // 1. Restricted Watchlist Broadcast State (loaded into Flink Enrichment)
      val watchlist = Map(
        "ENTITY-SHADOW-LTD" -> RestrictedEntityTag(
          entityId = "ENTITY-SHADOW-LTD",
          level = RestrictionLevel.SanctionedEntity,
          listSource = "EU-AML-SANCTIONS-2026",
          reasonDescription = "Sanctioned shell company linked to illicit financial flows"
        )
      )

      // 2. Ingest Verified External Documents via DocumentVerificationAgent -> EventPluginRegistry
      val aliceIncomeDoc = VerifiedDocumentFact(
        docId = "DOC-PAYSLIP-ALICE-10",
        customerId = "CUST-ALICE-001",
        factType = DocumentFactType.VerifiedMonthlyNetIncome,
        numericValue = Some(BigDecimal("4200.00")),
        textValue = Some("Permanent Full-Time Software Architect"),
        issuer = "National Tax & Payroll Registry",
        verifiedAt = now.minus(5, ChronoUnit.DAYS)
      )

      val bobIncomeDoc = VerifiedDocumentFact(
        docId = "DOC-PAYSLIP-BOB-10",
        customerId = "CUST-BOB-002",
        factType = DocumentFactType.VerifiedMonthlyNetIncome,
        numericValue = Some(BigDecimal("3500.00")),
        textValue = Some("Employed"),
        issuer = "National Tax & Payroll Registry",
        verifiedAt = now.minus(10, ChronoUnit.DAYS)
      )

      val charlieIncomeDoc = VerifiedDocumentFact(
        docId = "DOC-PAYSLIP-CHARLIE-10",
        customerId = "CUST-CHARLIE-003",
        factType = DocumentFactType.VerifiedMonthlyNetIncome,
        numericValue = Some(BigDecimal("4500.00")),
        textValue = Some("Senior Operations Manager"),
        issuer = "National Tax & Payroll Registry",
        verifiedAt = now.minus(8, ChronoUnit.DAYS)
      )

      Await.result(
        Future.sequence(
          Seq(
            gateway.ingestDocumentFact(aliceIncomeDoc),
            gateway.ingestDocumentFact(bobIncomeDoc),
            gateway.ingestDocumentFact(charlieIncomeDoc)
          )
        ),
        10.seconds
      )

      // 3. Initial Stream of Raw Transactions (Processed via Flink Validation + ExternalCategorizer + Bilateral Split + Watchlist)
      val initialRawTxs = Seq(
        RawTransaction(
          txId = "TX-SAL-ALICE-01",
          fromCustomerId = "EMPLOYER-CORP",
          toCounterpartyId = "CUST-ALICE-001",
          counterpartyIsBankClient = true,
          amount = BigDecimal("4200.00"),
          currency = "EUR",
          category = TransactionCategory.Salary,
          resultsInOverdraft = false,
          timestamp = now.minus(12, ChronoUnit.DAYS)
        ),
        RawTransaction(
          txId = "TX-RENT-ALICE-01",
          fromCustomerId = "CUST-ALICE-001",
          toCounterpartyId = "LANDLORD-PROP",
          counterpartyIsBankClient = false,
          amount = BigDecimal("1100.00"),
          currency = "EUR",
          category = TransactionCategory.Housing,
          resultsInOverdraft = false,
          timestamp = now.minus(10, ChronoUnit.DAYS)
        ),
        // Bilateral P2P transfer connecting Alice (CUST-ALICE-001) and Bob (CUST-BOB-002)
        RawTransaction(
          txId = "TX-PEER-ALICE-BOB-101",
          fromCustomerId = "CUST-ALICE-001",
          toCounterpartyId = "CUST-BOB-002",
          counterpartyIsBankClient = true,
          amount = BigDecimal("500.00"),
          currency = "EUR",
          category = TransactionCategory.PeerTransfer,
          resultsInOverdraft = false,
          timestamp = now.minus(3, ChronoUnit.DAYS)
        )
      )

      val initialEnriched = EnrichmentLogic.enrichBatchInMemory(initialRawTxs, watchlist)
      Await.result(Future.traverse(initialEnriched)(gateway.ingestTransaction), 10.seconds)

      // 4. Scenario 1: Level-3 Situation Projection & Formal Loan Evaluation for Alice while healthy
      val aliceLoan1 = LoanApplication(
        applicationId = "LOAN-APP-ALICE-001",
        customerId = "CUST-ALICE-001",
        requestedAmount = BigDecimal("12000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.065"),
        requestedAt = now.minus(2, ChronoUnit.HOURS)
      )

      val aliceProjection1 = Await.result(gateway.projectLoanSituation(aliceLoan1), 5.seconds)
      println(s"\n--- [LEVEL-3 SITUATION PROJECTION (Alice Healthy): Future DTI=${aliceProjection1.futureDti * 100}%, Affordable=${aliceProjection1.isAffordable}] ---")

      val aliceDecision1 = Await.result(gateway.evaluateLoan(aliceLoan1), 5.seconds)
      println("\n--- [SCENARIO 1: Alice Initial Loan Application (Healthy Situation)] ---")
      println(aliceDecision1.auditTrail.statementOfReasons)

      // 5. Real-Time Situational Shift:
      //    (a) Bob transacts with sanctioned entity ENTITY-SHADOW-LTD -> triggers 1st-hop flag on Bob
      //        AND reliable 2nd-hop Pekko actor risk propagation via RiskSignalAgent & Outbox to Alice!
      //    (b) Alice & Charlie both transact with CASINO-ROYALE-BET (categorized by Flink ExternalCategorizerModule)
      val situationalShiftRawTxs = Seq(
        RawTransaction(
          txId = "TX-SANCTION-BOB-999",
          fromCustomerId = "CUST-BOB-002",
          toCounterpartyId = "ENTITY-SHADOW-LTD",
          counterpartyIsBankClient = false,
          amount = BigDecimal("4800.00"),
          currency = "EUR",
          category = TransactionCategory.HighRiskTransfer,
          resultsInOverdraft = false,
          timestamp = now.minus(30, ChronoUnit.MINUTES)
        ),
        RawTransaction(
          txId = "TX-GAMB-ALICE-201",
          fromCustomerId = "CUST-ALICE-001",
          toCounterpartyId = "CASINO-ROYALE-BET",
          counterpartyIsBankClient = false,
          amount = BigDecimal("600.00"),
          currency = "EUR",
          category = TransactionCategory.EverydaySpend,
          resultsInOverdraft = true,
          timestamp = now.minus(20, ChronoUnit.MINUTES),
          merchantDescription = Some("CASINO ROYALE BETTING")
        ),
        RawTransaction(
          txId = "TX-GAMB-ALICE-202",
          fromCustomerId = "CUST-ALICE-001",
          toCounterpartyId = "CASINO-ROYALE-BET",
          counterpartyIsBankClient = false,
          amount = BigDecimal("500.00"),
          currency = "EUR",
          category = TransactionCategory.EverydaySpend,
          resultsInOverdraft = true,
          timestamp = now.minus(10, ChronoUnit.MINUTES),
          merchantDescription = Some("CASINO ROYALE BETTING")
        ),
        RawTransaction(
          txId = "TX-GAMB-CHARLIE-301",
          fromCustomerId = "CUST-CHARLIE-003",
          toCounterpartyId = "CASINO-ROYALE-BET",
          counterpartyIsBankClient = false,
          amount = BigDecimal("700.00"),
          currency = "EUR",
          category = TransactionCategory.EverydaySpend,
          resultsInOverdraft = false,
          timestamp = now.minus(5, ChronoUnit.MINUTES),
          merchantDescription = Some("CASINO ROYALE BETTING")
        )
      )

      val shiftEnriched = EnrichmentLogic.enrichBatchInMemory(situationalShiftRawTxs, watchlist)
      Await.result(Future.traverse(shiftEnriched)(gateway.ingestTransaction), 10.seconds)

      // Brief pause to allow asynchronous actor-to-actor 2nd-hop risk propagation and outbox confirmation to settle
      Thread.sleep(300)

      // 6. Execute Oracle Exadata Global Relationship Graph Analysis (SA_GRAPH -> NetworkRiskPatternDetected -> Agents)
      val globalPatterns = Await.result(gateway.runGlobalGraphAnalysis(now), 5.seconds)
      println(s"\n--- [ORACLE EXADATA GLOBAL GRAPH ANALYSIS: Detected ${globalPatterns.size} NetworkRiskPatternDetected Event(s)] ---")
      globalPatterns.foreach(p => println(s"  * ${p.patternId}: ${p.description} (score=${p.riskScore})"))

      // 7. Scenario 2: Bob applies for a loan -> Denied due to 1st-hop restricted entity match
      val bobLoan = LoanApplication(
        applicationId = "LOAN-APP-BOB-001",
        customerId = "CUST-BOB-002",
        requestedAmount = BigDecimal("10000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.065"),
        requestedAt = now
      )
      val bobDecision = Await.result(gateway.evaluateLoan(bobLoan), 5.seconds)
      println("\n--- [SCENARIO 2: Bob Loan Application (After Direct Restricted Counterparty Transfer)] ---")
      println(bobDecision.auditTrail.statementOfReasons)

      // 8. Scenario 3: Alice applies for a loan -> Denied with 2nd-hop contagion, global graph pattern, and cashflow shift
      val aliceLoan2 = LoanApplication(
        applicationId = "LOAN-APP-ALICE-002",
        customerId = "CUST-ALICE-001",
        requestedAmount = BigDecimal("18000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.065"),
        requestedAt = now
      )
      val aliceDecision2 = Await.result(gateway.evaluateLoan(aliceLoan2), 5.seconds)
      println("\n--- [SCENARIO 3: Alice Second Loan Application (2nd-Hop Contagion + Global Graph + Cashflow Shift)] ---")
      println(aliceDecision2.auditTrail.statementOfReasons)

      // 9. Print Hierarchical Situation Summary & Oracle Exadata 6-Schema Status
      val globalSituation = Await.result(gateway.getGlobalBankingSituation(now), 5.seconds)
      println("\n--- [HIERARCHICAL SITUATION AWARENESS & ORACLE EXADATA 6-SCHEMA SUMMARY] ---")
      println(s"  Active CustomerAgents    : ${globalSituation.activeCustomersCount}")
      println(s"  Active RelationshipAgents: ${globalSituation.activeRelationshipsCount}")
      println(s"  Active RiskSignals       : ${globalSituation.activeRiskSignalsCount}")
      println(s"  Verified Documents       : ${globalSituation.verifiedDocumentsCount}")
      println(s"  Global Graph (SA_GRAPH)  : ${globalSituation.globalGraphVerticesCount} vertices, ${globalSituation.globalGraphEdgesCount} edges")
      println(s"  Oracle Schemas Counts    : ${globalSituation.oracleSchemasStatus.map((k, v) => s"$k=$v").mkString(", ")}")
      println("=" * 96)

    finally
      system.terminate()
      Await.result(system.whenTerminated, 10.seconds)
