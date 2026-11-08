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

  "The Real-Time Banking Situation Awareness Architecture" should {

    "split bilateral transactions and tag restricted counterparties in the enrichment pipeline" in {
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

      val enriched = EnrichmentLogic.enrichBatchInMemory(Seq(p2pRaw, restrictedRaw), watchlist)

      // P2P between two bank clients produces 2 directed events; external produces 1
      enriched should have size 3
      val client1Out = enriched.find(e => e.txId == "TX-P2P-1" && e.customerId == "CLIENT-1").value
      client1Out.direction shouldBe TransactionDirection.Outflow
      client1Out.counterpartyId shouldBe "CLIENT-2"
      client1Out.restrictedTag shouldBe None

      val client2In = enriched.find(e => e.txId == "TX-P2P-1" && e.customerId == "CLIENT-2").value
      client2In.direction shouldBe TransactionDirection.Inflow
      client2In.counterpartyId shouldBe "CLIENT-1"

      val sanctionOut = enriched.find(_.txId == "TX-SANCTION-1").value
      sanctionOut.restrictedTag.map(_.entityId) shouldBe Some("SANCTIONED-ENTITY-77")
    }

    "approve a healthy customer and provide an explainable denial when 2nd-hop peer risk & overdrafts occur" in {
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

      // 1. Ingest verified income documents for Client A and Client B
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

      // 2. Client A receives salary and transfers money to Client B (creating a peer link)
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

      // 3. Evaluate initial loan for Client A -> Approved
      val loan1 = LoanApplication(
        applicationId = "APP-A-1",
        customerId = "CLIENT-A",
        requestedAmount = BigDecimal("10000.00"),
        termMonths = 24,
        annualInterestRate = BigDecimal("0.05"),
        requestedAt = now.minus(1, ChronoUnit.HOURS)
      )
      val decision1 = gateway.evaluateLoan(loan1).futureValue
      decision1.outcome shouldBe a[DecisionOutcome.Approved]

      // 4. Client B transacts with BLACKLIST-CORP-99 -> triggers 2nd-hop risk propagation to Client A
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

      // Verify Client A's actor receives the asynchronous 2nd-hop risk signal from Client B
      eventually {
        val stateA = gateway.getSituationState("CLIENT-A").futureValue
        stateA.riskExposures.values.exists(e => e.hopDistance == 2 && e.intermediaryPeerId.contains("CLIENT-B")) shouldBe true
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

    "serve loan origination evaluations and customer situation snapshots over Pekko HTTP and gRPC endpoints" in {
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
        snapshotDto.riskExposures.map(_.sourceEntityId) should contain("UNLICENSED-LENDER-88")

        // 5. Evaluate loan over gRPC path POST /com.bank.sa.proto.v1.LoanOriginationService/EvaluateLoan
        val loanReq = LoanOriginationRequest(
          applicationId = "LOAN-HTTP-01",
          customerId = "CLIENT-HTTP-1",
          requestedAmount = BigDecimal("8000.00"),
          termMonths = 24,
          annualInterestRate = BigDecimal("0.06"),
          requestedAt = Some(now)
        )
        val loanEntity = Marshal(loanReq).to[RequestEntity].futureValue
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

        // 6. Verify UI HTML endpoint (GET /) and System Overview Telemetry (GET /api/v1/system/overview)
        val uiResp = Http()(using system).singleRequest(HttpRequest(uri = s"$baseUrl/")).futureValue
        uiResp.status shouldBe StatusCodes.OK

        val overviewResp = Http()(using system)
          .singleRequest(HttpRequest(uri = s"$baseUrl/api/v1/system/overview"))
          .futureValue
        overviewResp.status shouldBe StatusCodes.OK
        val overviewDto = Unmarshal(overviewResp.entity).to[SystemOverviewDto].futureValue
        overviewDto.activeCustomerIds should contain("CLIENT-HTTP-1")
        overviewDto.workflowTraces.map(_.stage) should contain("FLINK_ENRICHMENT")
        overviewDto.workflowTraces.map(_.stage) should contain("PEKKO_SHARD_ACTOR")
        overviewDto.workflowTraces.map(_.stage) should contain("EXPLAINABLE_ENGINE")
      finally
        binding.unbind().futureValue
    }
  }
