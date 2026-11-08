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

      println("=" * 88)
      println(" REAL-TIME BANKING SITUATION AWARENESS & EXPLAINABLE LOAN DECISIONING (SCALA 3 + PEKKO)")
      println("=" * 88)

      // 1. Restricted Watchlist Broadcast State (loaded into Flink Enrichment)
      val watchlist = Map(
        "ENTITY-SHADOW-LTD" -> RestrictedEntityTag(
          entityId = "ENTITY-SHADOW-LTD",
          level = RestrictionLevel.SanctionedEntity,
          listSource = "EU-AML-SANCTIONS-2026",
          reasonDescription = "Sanctioned shell company linked to illicit financial flows"
        )
      )

      // 2. Ingest Verified External Documents (Payslips & External Liabilities)
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

      Await.result(
        Future.sequence(Seq(gateway.ingestDocumentFact(aliceIncomeDoc), gateway.ingestDocumentFact(bobIncomeDoc))),
        10.seconds
      )

      // 3. Initial Stream of Raw Transactions (Processed via Bilateral Splitting + Watchlist Enrichment)
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

      // 4. Scenario 1: Alice applies for a consumer loan while her situation is healthy
      val aliceLoan1 = LoanApplication(
        applicationId = "LOAN-APP-ALICE-001",
        customerId = "CUST-ALICE-001",
        requestedAmount = BigDecimal("12000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.065"),
        requestedAt = now.minus(2, ChronoUnit.HOURS)
      )

      val aliceDecision1 = Await.result(gateway.evaluateLoan(aliceLoan1), 5.seconds)
      println("\n--- [SCENARIO 1: Alice Initial Loan Application (Healthy Situation)] ---")
      println(aliceDecision1.auditTrail.statementOfReasons)

      // 5. Real-Time Situational Shift:
      //    (a) Bob transacts with sanctioned entity ENTITY-SHADOW-LTD -> triggers 1st-hop flag on Bob
      //        AND automatic 2nd-hop Pekko actor risk propagation to connected peer Alice!
      //    (b) Alice experiences sudden high-risk gambling outflows and 2 overdraft events.
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
          category = TransactionCategory.Gambling,
          resultsInOverdraft = true,
          timestamp = now.minus(20, ChronoUnit.MINUTES)
        ),
        RawTransaction(
          txId = "TX-GAMB-ALICE-202",
          fromCustomerId = "CUST-ALICE-001",
          toCounterpartyId = "CASINO-ROYALE-BET",
          counterpartyIsBankClient = false,
          amount = BigDecimal("500.00"),
          currency = "EUR",
          category = TransactionCategory.Gambling,
          resultsInOverdraft = true,
          timestamp = now.minus(10, ChronoUnit.MINUTES)
        )
      )

      val shiftEnriched = EnrichmentLogic.enrichBatchInMemory(situationalShiftRawTxs, watchlist)
      Await.result(Future.traverse(shiftEnriched)(gateway.ingestTransaction), 10.seconds)

      // Brief pause to allow asynchronous actor-to-actor 2nd-hop risk propagation to settle
      Thread.sleep(300)

      // 6. Scenario 2: Bob applies for a loan -> Denied due to 1st-hop restricted entity match
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

      // 7. Scenario 3: Alice applies for a loan minutes after Bob's restricted transaction & her liquidity shock
      val aliceLoan2 = LoanApplication(
        applicationId = "LOAN-APP-ALICE-002",
        customerId = "CUST-ALICE-001",
        requestedAmount = BigDecimal("18000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.065"),
        requestedAt = now
      )
      val aliceDecision2 = Await.result(gateway.evaluateLoan(aliceLoan2), 5.seconds)
      println("\n--- [SCENARIO 3: Alice Second Loan Application (Real-Time 2nd-Hop Contagion & Cashflow Shift)] ---")
      println(aliceDecision2.auditTrail.statementOfReasons)
      println("=" * 88)

    finally
      system.terminate()
      Await.result(system.whenTerminated, 10.seconds)

