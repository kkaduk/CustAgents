package com.bank.sa

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.api.*
import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import com.bank.sa.enrichment.EnrichmentLogic
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.compiletime.uninitialized
import scala.concurrent.Future
import scala.concurrent.duration.*

class CustomerSituationAwarenessSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.load())
    with AnyWordSpecLike
    with Eventually
    with OptionValues:

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(5.seconds), interval = scaled(100.millis))

  private val creditEngine = new ExplainableCreditEngine()
  private var gateway: CustomerClusterGateway = uninitialized

  override def beforeAll(): Unit =
    super.beforeAll()
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)
    gateway = new CustomerClusterGateway(system, creditEngine)

  "The Real-Time Banking Situation Awareness Architecture (Apache Pekko + Oracle Exadata)" should {

    "validate, categorize via ExternalTransactionCategorizer, split bilateral transactions, and tag restricted counterparties in Flink" in {
      val now = Instant.parse("2026-11-07T12:00:00Z")
      val watchlist = Map(
        "SANCTIONED-ENTITY-77" -> RestrictedEntityTag(
          entityId = "SANCTIONED-ENTITY-77",
          level = RestrictionLevel.SanctionedEntity,
          listSource = "OFAC-EU-CONSOLIDATED",
          reasonDescription = "Sanctioned financial conduit"
        )
      )

      val p2pRaw = RawTransaction(
        txId = "TX-P2P-1",
        fromCustomerId = "CLIENT-1",
        toCounterpartyId = "CLIENT-2",
        counterpartyIsBankClient = true,
        amount = BigDecimal("250.00"),
        currency = "EUR",
        category = TransactionCategory.PeerTransfer,
        resultsInOverdraft = false,
        timestamp = now
      )

      val uncategorizedGamblingRaw = RawTransaction(
        txId = "TX-CASINO-1",
        fromCustomerId = "CLIENT-1",
        toCounterpartyId = "ROYAL-CASINO-HUB",
        counterpartyIsBankClient = false,
        amount = BigDecimal("400.00"),
        currency = "EUR",
        category = TransactionCategory.EverydaySpend, // ExternalCategorizerModule should classify this as Gambling
        resultsInOverdraft = false,
        timestamp = now,
        merchantDescription = Some("ROYAL CASINO BETTING")
      )

      val restrictedRaw = RawTransaction(
        txId = "TX-SANCTION-1",
        fromCustomerId = "CLIENT-2",
        toCounterpartyId = "SANCTIONED-ENTITY-77",
        counterpartyIsBankClient = false,
        amount = BigDecimal("1500.00"),
        currency = "EUR",
        category = TransactionCategory.HighRiskTransfer,
        resultsInOverdraft = false,
        timestamp = now
      )

      val enriched = EnrichmentLogic.enrichBatchInMemory(Seq(p2pRaw, uncategorizedGamblingRaw, restrictedRaw), watchlist)

      // P2P between two bank clients produces 2 directed events; two external txs produce 1 each -> total 4
      enriched should have size 4
      val client1Out = enriched.find(e => e.txId == "TX-P2P-1" && e.customerId == "CLIENT-1").value
      client1Out.direction shouldBe TransactionDirection.Outflow
      client1Out.counterpartyId shouldBe "CLIENT-2"
      client1Out.restrictedTag shouldBe None

      val client2In = enriched.find(e => e.txId == "TX-P2P-1" && e.customerId == "CLIENT-2").value
      client2In.direction shouldBe TransactionDirection.Inflow
      client2In.counterpartyId shouldBe "CLIENT-1"

      val casinoEnriched = enriched.find(_.txId == "TX-CASINO-1").value
      casinoEnriched.category shouldBe TransactionCategory.Gambling
      casinoEnriched.categorizerSource should include("ExternalCategorizerModule")

      val sanctionOut = enriched.find(_.txId == "TX-SANCTION-1").value
      sanctionOut.restrictedTag.map(_.entityId) shouldBe Some("SANCTIONED-ENTITY-77")
    }

    "route observed events through EventPluginRegistry and guarantee Idempotency, Transactional Outbox, and Situation Consistency" in {
      val now = Instant.parse("2026-11-07T13:00:00Z")
      val registry = EventPluginRegistry.default

      registry.registeredPluginIds should contain allOf (
        "plugin.sa.transaction-observed.v1",
        "plugin.sa.income-verified.v1",
        "plugin.sa.debt-updated.v1",
        "plugin.sa.counterparty-risk-received.v1",
        "plugin.sa.external-document-received.v1",
        "plugin.sa.global-risk-intelligence.v1"
      )

      val initialState = CustomerSituationState.empty("CUST-PLUGIN-TEST")
      val incomeDoc = VerifiedDocumentFact(
        docId = "DOC-INC-100",
        customerId = "CUST-PLUGIN-TEST",
        factType = DocumentFactType.VerifiedMonthlyNetIncome,
        numericValue = Some(BigDecimal("5000.00")),
        textValue = Some("Permanent Contract"),
        issuer = "TaxRegistry",
        verifiedAt = now
      )
      val incomeEvent = IncomeVerified("DOC-INC-100", "CUST-PLUGIN-TEST", incomeDoc, BigDecimal("5000.00"), now)
      val res1        = registry.routeAndProcess(initialState, incomeEvent)

      res1.situationEvaluated.handledByPluginId shouldBe "plugin.sa.income-verified.v1"
      res1.updatedState.situationSummary.monthlyIncome shouldBe BigDecimal("5000.00")
      res1.updatedState.situationSummary.incomeStability shouldBe IncomeStability.High
      res1.updatedState.dataCompleteness.hasVerifiedIncome shouldBe true
      res1.updatedState.stateVersion shouldBe 2L

      // Verify idempotency: re-recording the same document or transaction does not increment stateVersion or double-count
      val duplicateState = res1.updatedState.recordDocumentFact(incomeDoc)
      duplicateState.stateVersion shouldBe res1.updatedState.stateVersion
    }

    "approve a healthy customer, compute Level-3 Situation Projections, and provide an explainable denial when 2nd-hop peer risk & overdrafts occur" in {
      given scala.concurrent.ExecutionContext = system.executionContext
      val now = Instant.parse("2026-11-07T15:00:00Z")

      val watchlist = Map(
        "BLACKLIST-CORP-99" -> RestrictedEntityTag(
          entityId = "BLACKLIST-CORP-99",
          level = RestrictionLevel.SanctionedEntity,
          listSource = "EU-SANCTIONS-LIST",
          reasonDescription = "Prohibited counterparty"
        )
      )

      // 1. Ingest verified income documents for Client A and Client B via DocumentVerificationAgent
      val docA = VerifiedDocumentFact(
        docId = "DOC-INC-A",
        customerId = "CLIENT-A",
        factType = DocumentFactType.VerifiedMonthlyNetIncome,
        numericValue = Some(BigDecimal("5000.00")),
        textValue = Some("Verified Full-Time Salary"),
        issuer = "TaxAuthority",
        verifiedAt = now.minus(5, ChronoUnit.DAYS)
      )
      gateway.ingestDocumentFact(docA).futureValue

      // 2. Client A receives salary and transfers money to Client B (creating a peer link & RelationshipAgent state)
      val baseTxs = EnrichmentLogic.enrichBatchInMemory(
        Seq(
          RawTransaction(
            txId = "TX-SAL-A",
            fromCustomerId = "EMPLOYER-1",
            toCounterpartyId = "CLIENT-A",
            counterpartyIsBankClient = true,
            amount = BigDecimal("5000.00"),
            currency = "EUR",
            category = TransactionCategory.Salary,
            resultsInOverdraft = false,
            timestamp = now.minus(4, ChronoUnit.DAYS)
          ),
          RawTransaction(
            txId = "TX-LINK-AB",
            fromCustomerId = "CLIENT-A",
            toCounterpartyId = "CLIENT-B",
            counterpartyIsBankClient = true,
            amount = BigDecimal("400.00"),
            currency = "EUR",
            category = TransactionCategory.PeerTransfer,
            resultsInOverdraft = false,
            timestamp = now.minus(2, ChronoUnit.DAYS)
          )
        ),
        watchlist
      )
      Future.traverse(baseTxs)(gateway.ingestTransaction).futureValue

      // 3. Compute Level-3 Situation Projection & Evaluate initial loan for Client A -> Approved
      val loan1 = LoanApplication(
        applicationId = "APP-A-1",
        customerId = "CLIENT-A",
        requestedAmount = BigDecimal("10000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.05"),
        requestedAt = now.minus(1, ChronoUnit.HOURS)
      )
      val projection1 = gateway.projectLoanSituation(loan1).futureValue
      projection1.isAffordable shouldBe true
      projection1.futureDti should be < BigDecimal("0.40")

      val decision1 = gateway.evaluateLoan(loan1).futureValue
      decision1.outcome shouldBe a[DecisionOutcome.Approved]
      decision1.auditTrail.situationProjection shouldBe defined

      // 4. Client B transacts with BLACKLIST-CORP-99 -> triggers 2nd-hop risk propagation via RiskSignalAgent & Outbox to Client A
      val riskyTxs = EnrichmentLogic.enrichBatchInMemory(
        Seq(
          RawTransaction(
            txId = "TX-B-RESTRICTED",
            fromCustomerId = "CLIENT-B",
            toCounterpartyId = "BLACKLIST-CORP-99",
            counterpartyIsBankClient = false,
            amount = BigDecimal("3000.00"),
            currency = "EUR",
            category = TransactionCategory.HighRiskTransfer,
            resultsInOverdraft = false,
            timestamp = now.minus(15, ChronoUnit.MINUTES)
          )
        ),
        watchlist
      )
      Future.traverse(riskyTxs)(gateway.ingestTransaction).futureValue

      // Verify Client A's actor receives the reliable 2nd-hop risk signal and Client B's Transactional Outbox is confirmed
      eventually {
        val stateA = gateway.getSituationState("CLIENT-A").futureValue
        stateA.riskExposures.values.exists(e => e.hopDistance == 2 && e.intermediaryPeerId.contains("CLIENT-B")) shouldBe true

        val stateB = gateway.getSituationState("CLIENT-B").futureValue
        stateB.pendingOutbox shouldBe empty
      }

      // 5. Client A applies for a second loan -> Denied with exact 2nd-hop contagion reason code & evidence IDs
      val loan2 = LoanApplication(
        applicationId = "APP-A-2",
        customerId = "CLIENT-A",
        requestedAmount = BigDecimal("10000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.05"),
        requestedAt = now
      )
      val decision2 = gateway.evaluateLoan(loan2).futureValue
      decision2.outcome shouldBe a[DecisionOutcome.Denied]

      val denied = decision2.outcome.asInstanceOf[DecisionOutcome.Denied]
      val contagionReason = denied.principalReasons.find(_.code == "RC_302_INDIRECT_NETWORK_RISK_CONTAGION").value
      contagionReason.evidenceIds should contain("TX-LINK-AB")
      contagionReason.evidenceIds should contain("TX-B-RESTRICTED")
      denied.statementOfReasons should include("RC_302_INDIRECT_NETWORK_RISK_CONTAGION")
    }

    "execute the Oracle Exadata Global Relationship Graph cycle (SA_GRAPH -> NetworkRiskPatternDetected -> CustomerAgent & PortfolioRiskAgent)" in {
      given scala.concurrent.ExecutionContext = system.executionContext
      val now = Instant.parse("2026-11-07T15:30:00Z")

      val watchlist = Map(
        "SHARED-OFFSHORE-HUB" -> RestrictedEntityTag(
          entityId = "SHARED-OFFSHORE-HUB",
          level = RestrictionLevel.FraudWatchlist,
          listSource = "GLOBAL-GRAPH-WATCH",
          reasonDescription = "Shared offshore shell entity"
        )
      )

      // Two seemingly independent clients (CLIENT-G1 and CLIENT-G2) both transact with SHARED-OFFSHORE-HUB
      val graphTxs = EnrichmentLogic.enrichBatchInMemory(
        Seq(
          RawTransaction(
            txId = "TX-G1-HUB",
            fromCustomerId = "CLIENT-G1",
            toCounterpartyId = "SHARED-OFFSHORE-HUB",
            counterpartyIsBankClient = false,
            amount = BigDecimal("900.00"),
            currency = "EUR",
            category = TransactionCategory.HighRiskTransfer,
            resultsInOverdraft = false,
            timestamp = now.minus(10, ChronoUnit.MINUTES)
          ),
          RawTransaction(
            txId = "TX-G2-HUB",
            fromCustomerId = "CLIENT-G2",
            toCounterpartyId = "SHARED-OFFSHORE-HUB",
            counterpartyIsBankClient = false,
            amount = BigDecimal("1100.00"),
            currency = "EUR",
            category = TransactionCategory.HighRiskTransfer,
            resultsInOverdraft = false,
            timestamp = now.minus(5, ChronoUnit.MINUTES)
          )
        ),
        watchlist
      )
      Future.traverse(graphTxs)(gateway.ingestTransaction).futureValue

      val detectedPatterns = gateway.runGlobalGraphAnalysis(now).futureValue
      detectedPatterns.map(_.sharedEntityId) should contain("SHARED-OFFSHORE-HUB")

      val stateG1 = gateway.getSituationState("CLIENT-G1").futureValue
      stateG1.riskExposures.values.exists(e => e.hopDistance == 3 && e.sourceEntityId == "SHARED-OFFSHORE-HUB") shouldBe true

      val globalSituation = gateway.getGlobalBankingSituation(now).futureValue
      globalSituation.activeCustomersCount should be >= 2
      globalSituation.oracleSchemasStatus.keySet should contain allOf (
        "SA_PERSISTENCE",
        "SA_FACTS",
        "SA_GRAPH",
        "SA_INTELLIGENCE",
        "SA_DECISIONS",
        "SA_GOVERNANCE"
      )
    }

    "serve loan origination evaluations, projections, Oracle Exadata 6-schema overview, and hierarchy over Pekko HTTP and gRPC endpoints" in {
      import JacksonJsonSupport.given
      given scala.concurrent.ExecutionContext = system.executionContext
      val now = Instant.parse("2026-11-07T16:00:00Z")

      val watchlist = Map(
        "UNLICENSED-LENDER-88" -> RestrictedEntityTag(
          entityId = "UNLICENSED-LENDER-88",
          level = RestrictionLevel.UnlicensedLender,
          listSource = "NATIONAL-REGULATOR-WATCHLIST",
          reasonDescription = "Unlicensed predatory credit provider"
        )
      )

      val server  = new LoanOriginationServer(gateway, () => watchlist)
      val binding = server.start("127.0.0.1", 0).futureValue
      val baseUrl = s"http://127.0.0.1:${binding.localAddress.getPort}"

      try
        // 1. Check /health endpoint
        val healthResp = Http()(using system).singleRequest(HttpRequest(uri = s"$baseUrl/health")).futureValue
        healthResp.status shouldBe StatusCodes.OK
        val healthDto = Unmarshal(healthResp.entity).to[HealthStatusDto].futureValue
        healthDto.status shouldBe "UP"

        // 2. Upload verified monthly income document via POST /api/v1/customers/{id}/documents
        val docUpload = DocumentFactUploadRequest(
          docId = "DOC-HTTP-01",
          factType = "VerifiedMonthlyNetIncome",
          numericValue = Some(BigDecimal("4000.00")),
          textValue = Some("Permanent Contract"),
          issuer = "EmployerPayroll",
          verifiedAt = Some(now.minus(3, ChronoUnit.DAYS))
        )
        val docEntity = Marshal(docUpload).to[RequestEntity].futureValue
        val docResp = Http()(using system)
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = s"$baseUrl/api/v1/customers/CLIENT-HTTP-1/documents",
              entity = docEntity
            )
          )
          .futureValue
        docResp.status shouldBe StatusCodes.OK

        // 3. Ingest a high-risk transaction with UNLICENSED-LENDER-88 via POST /api/v1/transactions/ingest
        val txReq = RawTransactionIngestRequest(
          txId = "TX-HTTP-RISK-01",
          fromCustomerId = "CLIENT-HTTP-1",
          toCounterpartyId = "UNLICENSED-LENDER-88",
          counterpartyIsBankClient = false,
          amount = BigDecimal("1200.00"),
          currency = "EUR",
          category = "HighRiskTransfer",
          resultsInOverdraft = false,
          timestamp = Some(now.minus(1, ChronoUnit.HOURS))
        )
        val txEntity = Marshal(txReq).to[RequestEntity].futureValue
        val txResp = Http()(using system)
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = s"$baseUrl/api/v1/transactions/ingest",
              entity = txEntity
            )
          )
          .futureValue
        txResp.status shouldBe StatusCodes.OK

        // 4. Query situation snapshot via GET /api/v1/customers/CLIENT-HTTP-1/situation
        val sitResp = Http()(using system)
          .singleRequest(HttpRequest(uri = s"$baseUrl/api/v1/customers/CLIENT-HTTP-1/situation"))
          .futureValue
        sitResp.status shouldBe StatusCodes.OK
        val snapshotDto = Unmarshal(sitResp.entity).to[CustomerSituationSnapshotDto].futureValue
        snapshotDto.customerId shouldBe "CLIENT-HTTP-1"
        snapshotDto.outflow30d shouldBe BigDecimal("1200.00")
        snapshotDto.stateVersion should be >= 2L
        snapshotDto.riskExposures.map(_.sourceEntityId) should contain("UNLICENSED-LENDER-88")

        // 5. Project loan scenario via POST /api/v1/loans/project
        val loanReq = LoanOriginationRequest(
          applicationId = "LOAN-HTTP-01",
          customerId = "CLIENT-HTTP-1",
          requestedAmount = BigDecimal("8000.00"),
          termMonths = 24,
          annualInterestRate = BigDecimal("0.06"),
          requestedAt = Some(now)
        )
        val loanEntity = Marshal(loanReq).to[RequestEntity].futureValue
        val projResp = Http()(using system)
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = s"$baseUrl/api/v1/loans/project",
              entity = loanEntity
            )
          )
          .futureValue
        projResp.status shouldBe StatusCodes.OK
        val projDto = Unmarshal(projResp.entity).to[SituationProjectionDto].futureValue
        projDto.customerId shouldBe "CLIENT-HTTP-1"
        projDto.stateVersion should be >= 2L

        // 6. Evaluate loan over gRPC path POST /com.bank.sa.proto.v1.LoanOriginationService/EvaluateLoan
        val evalResp = Http()(using system)
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = s"$baseUrl/com.bank.sa.proto.v1.LoanOriginationService/EvaluateLoan",
              entity = loanEntity
            )
          )
          .futureValue
        evalResp.status shouldBe StatusCodes.OK
        val decisionDto = Unmarshal(evalResp.entity).to[LoanOriginationDecisionDto].futureValue
        decisionDto.status shouldBe "DENIED"
        decisionDto.adverseActionReasons.map(_.code) should contain("RC_301_DIRECT_RESTRICTED_COUNTERPARTY")
        decisionDto.adverseActionReasons.flatMap(_.evidenceIds) should contain("TX-HTTP-RISK-01")

        // 7. Verify Oracle Exadata 6-Schema Overview (GET /api/v1/oracle/overview) & Agent Hierarchy (GET /api/v1/agents/hierarchy)
        val oracleResp = Http()(using system)
          .singleRequest(HttpRequest(uri = s"$baseUrl/api/v1/oracle/overview"))
          .futureValue
        oracleResp.status shouldBe StatusCodes.OK
        val oracleDto = Unmarshal(oracleResp.entity).to[OracleExadataOverviewDto].futureValue
        oracleDto.schemasRecordCounts.keys should contain("SA_PERSISTENCE")
        oracleDto.schemasRecordCounts.keys should contain("SA_FACTS")
        oracleDto.schemasRecordCounts.keys should contain("SA_GRAPH")

        val hierarchyResp = Http()(using system)
          .singleRequest(HttpRequest(uri = s"$baseUrl/api/v1/agents/hierarchy"))
          .futureValue
        hierarchyResp.status shouldBe StatusCodes.OK

        // 8. Verify UI HTML endpoint (GET /) and System Overview Telemetry (GET /api/v1/system/overview)
        val uiResp = Http()(using system).singleRequest(HttpRequest(uri = s"$baseUrl/")).futureValue
        uiResp.status shouldBe StatusCodes.OK

        val overviewResp = Http()(using system)
          .singleRequest(HttpRequest(uri = s"$baseUrl/api/v1/system/overview"))
          .futureValue
        overviewResp.status shouldBe StatusCodes.OK
        val overviewDto = Unmarshal(overviewResp.entity).to[SystemOverviewDto].futureValue
        overviewDto.activeCustomerIds should contain("CLIENT-HTTP-1")
        overviewDto.registeredEventPlugins should not be empty
        overviewDto.workflowTraces.map(_.stage) should contain("FLINK_ENRICHMENT")
        overviewDto.workflowTraces.map(_.stage) should contain("PEKKO_SHARD_ACTOR")
        overviewDto.workflowTraces.map(_.stage) should contain("EVENT_PLUGIN_ENGINE")
        overviewDto.workflowTraces.map(_.stage) should contain("EXPLAINABLE_ENGINE")
      finally
        binding.unbind().futureValue
    }

    "enforce all Section 9 Roadmap improvements: 64-bit event digests, bounded super-node Ego-Network, sharded Risk/Portfolio coordinators, Outbox retry sweeper, batched journal writes, and SQL/PGQ Graph query pushdown" in {
      val now = Instant.parse("2026-11-07T17:00:00Z")

      // 1. Verify 64-bit collision-resistant event digest (Roadmap 9.5)
      val d1 = CustomerSituationState.digest64("TX-10001")
      val d2 = CustomerSituationState.digest64("TX-10002")
      d1 should not be d2
      var state = CustomerSituationState.empty("CUST-SUPER-NODE").copy(maxEgoNetworkPeers = 5)
      state = state.markEventProcessed("HIST-EVT-999")
      state.processedEventDigests should contain(CustomerSituationState.digest64("HIST-EVT-999"))
      state.hasProcessedEvent("HIST-EVT-999") shouldBe true

      // 2. Verify Bounded Ego-Network & High-Degree Super-Node fan-out protection (Roadmap 9.4)
      val plugin = new TransactionObservedPlugin(secondHopAttenuation = 0.65, maxPushPeers = 3)
      (1 to 8).foreach { idx =>
        val peerTx = EnrichedTransaction(
          txId = s"TX-PEER-SN-$idx",
          customerId = "CUST-SUPER-NODE",
          counterpartyId = s"CUST-PEER-$idx",
          counterpartyIsBankClient = true,
          direction = TransactionDirection.Outflow,
          amount = BigDecimal(idx * 100),
          currency = "EUR",
          category = TransactionCategory.PeerTransfer,
          resultsInOverdraft = false,
          restrictedTag = None,
          timestamp = now.plusSeconds(idx.toLong)
        )
        state = state.recordTransaction(peerTx)
      }
      // Ego-network is strictly bounded to maxEgoNetworkPeers = 5
      state.egoNetwork.size shouldBe 5
      state.isHighDegreeSuperNode shouldBe true

      // When a 1st-hop restricted hit occurs on the super-node, proactive push outbox entries are capped at maxPushPeers = 3
      val restrictedTx = EnrichedTransaction(
        txId = "TX-SN-SANCTION-99",
        customerId = "CUST-SUPER-NODE",
        counterpartyId = "SANCTION-HUB-X",
        counterpartyIsBankClient = false,
        direction = TransactionDirection.Outflow,
        amount = BigDecimal("2500.00"),
        currency = "EUR",
        category = TransactionCategory.HighRiskTransfer,
        resultsInOverdraft = false,
        restrictedTag = Some(
          RestrictedEntityTag("SANCTION-HUB-X", RestrictionLevel.SanctionedEntity, "EU-AML", "Sanctioned hub")
        ),
        timestamp = now.plusSeconds(30)
      )
      val pluginResult = plugin.process(
        state,
        TransactionObserved("TX-SN-SANCTION-99", "CUST-SUPER-NODE", restrictedTx, restrictedTx.timestamp)
      )
      val peerOutboxSignals = pluginResult.outboxEntries.flatMap(_.peerSignal)
      peerOutboxSignals.size shouldBe 3

      // 3. Verify Sharded RiskSignalAgent & Sharded PortfolioRiskAgent Sub-Segment Merging (Roadmap 9.2)
      val coordKey1 = RiskSignalAgentState.coordinatorKeyFor("SIG-1")
      val coordKey2 = RiskSignalAgentState.coordinatorKeyFor("SIG-2")
      coordKey1 should startWith("RISK-SIGNAL-SHARD-")
      coordKey2 should startWith("RISK-SIGNAL-SHARD-")

      val subSegKey = PortfolioSituationState.subSegmentKeyFor("RETAIL-PRIME", "CUST-SUPER-NODE")
      subSegKey should startWith("RETAIL-PRIME#SHARD-")
      val shardA = PortfolioSituationState(subSegKey).updateCustomerEntry(pluginResult.updatedState)
      val mergedPortfolio = PortfolioSituationState.mergeSubSegments("RETAIL-PRIME", List(shardA))
      mergedPortfolio.activeCustomersCount shouldBe 1
      mergedPortfolio.highRiskCustomerCount shouldBe 1

      // 4. Verify Active Outbox Retry Sweeper command on CustomerAgent (Roadmap 9.3)
      gateway.entityFor("CLIENT-A") ! com.bank.sa.agent.CustomerAgent.RetryPendingOutbox
      val stateAfterSweep = gateway.getSituationState("CLIENT-A").futureValue
      stateAfterSweep.pendingOutbox shouldBe empty

      // 5. Verify Batched Journal Writes & SQL/PGQ GRAPH_TABLE query in OracleExadataRepository (Roadmap 9.1 & 9.6)
      val batchRows = gateway.oracleRepository.appendJournalEventsBatch(
        "CustomerAgent:CUST-BATCH-1",
        Seq(
          ("TransactionObserved", "Batch Event 1", now),
          ("SituationEvaluated", "Batch Event 2", now)
        )
      )
      batchRows should have size 2
      gateway.oracleRepository.GlobalGraphPgqSql should include("GRAPH_TABLE")
      gateway.oracleRepository.GlobalGraphPgqSql should include("SA_GRAPH.SA_GLOBAL_RELATIONSHIP_GRAPH")
    }
  }
