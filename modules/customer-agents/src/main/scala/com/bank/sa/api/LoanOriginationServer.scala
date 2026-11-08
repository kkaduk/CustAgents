package com.bank.sa.api

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.domain.*
import com.bank.sa.enrichment.EnrichmentLogic
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.model.headers.CacheDirectives.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success, Using}

/**
 * Pekko HTTP & gRPC Gateway Layer for Real-Time Loan Origination, Interactive Business UI,
 * and Internal Agent / Workflow Investigation.
 */
class LoanOriginationServer(
    gateway: CustomerClusterGateway,
    watchlistProvider: () => Map[String, RestrictedEntityTag] = () => LoanOriginationServer.defaultWatchlist
)(using system: ActorSystem[_]):

  import JacksonJsonSupport.given
  private given ExecutionContext = system.executionContext

  val grpcService: LoanOriginationGrpcService = new LoanOriginationGrpcServiceImpl(gateway)

  private val corsHeaders = List(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Credentials`(true),
    `Access-Control-Allow-Headers`("Authorization", "Content-Type", "X-Requested-With"),
    `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS, HttpMethods.DELETE)
  )

  val routes: Route =
    respondWithHeaders(corsHeaders) {
      concat(
        options {
          complete(HttpResponse(StatusCodes.OK))
        },

        // 0. Interactive Business & Agent Inspector UI (with no-cache headers so browser always loads latest UI)
        pathSingleSlash {
          get {
            respondWithHeader(`Cache-Control`(`no-cache`, `no-store`, `must-revalidate`)) {
              val devFile = new java.io.File("modules/customer-agents/src/main/resources/ui/index.html")
              if devFile.exists() then getFromFile(devFile, ContentTypes.`text/html(UTF-8)`)
              else getFromResource("ui/index.html", ContentTypes.`text/html(UTF-8)`)
            }
          }
        },
        path("ui") {
          get {
            respondWithHeader(`Cache-Control`(`no-cache`, `no-store`, `must-revalidate`)) {
              val devFile = new java.io.File("modules/customer-agents/src/main/resources/ui/index.html")
              if devFile.exists() then getFromFile(devFile, ContentTypes.`text/html(UTF-8)`)
              else getFromResource("ui/index.html", ContentTypes.`text/html(UTF-8)`)
            }
          }
        },

        // 1. Health & Cluster Readiness Endpoint
        path("health") {
          get {
            val memberStatus = Cluster(system).selfMember.status.toString
            complete(
              StatusCodes.OK,
              HealthStatusDto(
                service = "banking-situation-awareness-api",
                status = "UP",
                clusterMemberStatus = memberStatus
              )
            )
          }
        },

        // 2. REST API v1 Endpoints
        pathPrefix("api" / "v1") {
          concat(
            // GET /api/v1/system/overview — Full cluster, agent, and workflow trace inspector
            path("system" / "overview") {
              get {
                val now         = Instant.now()
                val customerIds = gateway.getActiveCustomerIds
                val snapshotsFuture = Future.traverse(customerIds) { id =>
                  grpcService.getCustomerSituation(id, now)
                }
                onComplete(snapshotsFuture) {
                  case Success(snapshots) =>
                    val overview = SystemOverviewDto(
                      activeCustomerIds = customerIds,
                      customers = snapshots,
                      watchlist = watchlistProvider().values.toList.map(WatchlistEntryDto.fromDomain),
                      workflowTraces = gateway.getWorkflowTraces,
                      recentDecisions = gateway.getRecentDecisions
                    )
                    complete(StatusCodes.OK, overview)
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                }
              }
            },

            // POST /api/v1/demo/scenario/{step} — Interactive Guided Demo Steps for Business Users
            path("demo" / "scenario" / Segment) { stepName =>
              post {
                onComplete(runDemoStep(stepName)) {
                  case Success(msg) => complete(StatusCodes.OK, Map("status" -> "EXECUTED", "step" -> stepName, "message" -> msg))
                  case Failure(ex)  => complete(StatusCodes.BadRequest, Map("error" -> ex.getMessage))
                }
              }
            },

            // POST /api/v1/loans/evaluate
            path("loans" / "evaluate") {
              post {
                entity(as[LoanOriginationRequest]) { request =>
                  onComplete(grpcService.evaluateLoan(request)) {
                    case Success(decisionDto) => complete(StatusCodes.OK, decisionDto)
                    case Failure(ex)          => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                  }
                }
              }
            },

            // /api/v1/customers/{customerId}/...
            pathPrefix("customers" / Segment) { customerId =>
              concat(
                // GET /api/v1/customers/{customerId}/situation
                path("situation") {
                  get {
                    onComplete(grpcService.getCustomerSituation(customerId, Instant.now())) {
                      case Success(snapshotDto) => complete(StatusCodes.OK, snapshotDto)
                      case Failure(ex)          => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                    }
                  }
                },

                // POST /api/v1/customers/{customerId}/documents
                path("documents") {
                  post {
                    entity(as[DocumentFactUploadRequest]) { docRequest =>
                      onComplete(grpcService.ingestDocumentFact(customerId, docRequest)) {
                        case Success(ack) => complete(StatusCodes.OK, ack)
                        case Failure(ex)  => complete(StatusCodes.BadRequest, Map("error" -> ex.getMessage))
                      }
                    }
                  }
                }
              )
            },

            // POST /api/v1/transactions/ingest
            path("transactions" / "ingest") {
              post {
                entity(as[RawTransactionIngestRequest]) { rawTxReq =>
                  val rawDomain = rawTxReq.toDomain
                  val enrichedList = EnrichmentLogic.enrichBatchInMemory(
                    Seq(rawDomain),
                    watchlistProvider()
                  )

                  gateway.recordTrace(
                    stage = "FLINK_ENRICHMENT",
                    fromComponent = "Kafka(tx.raw.v1)",
                    toComponent = "Flink(BilateralSplitter + WatchlistBroadcast)",
                    eventType = if enrichedList.size > 1 then "BilateralSplit (2 Perspectives)" else "SinglePerspective",
                    summary =
                      s"Flink enriched raw tx '${rawDomain.txId}' (${rawDomain.fromCustomerId} -> ${rawDomain.toCounterpartyId}, ${rawDomain.amount} EUR) into ${enrichedList.size} sharded event(s)",
                    details = Map(
                      "txId" -> rawDomain.txId,
                      "perspectivesEmitted" -> enrichedList.map(e => s"${e.customerId}:${e.direction}").mkString(", "),
                      "watchlistHit" -> enrichedList.flatMap(_.restrictedTag).headOption.map(_.entityId).getOrElse("None")
                    ),
                    timestamp = rawDomain.timestamp
                  )

                  val ingestFuture = Future
                    .traverse(enrichedList)(gateway.ingestTransaction)
                    .map(_.map(ack => IngestionAckDto(ack.customerId, ack.referenceId)))

                  onComplete(ingestFuture) {
                    case Success(acks) => complete(StatusCodes.OK, acks)
                    case Failure(ex)   => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                  }
                }
              }
            }
          )
        },

        // 3. gRPC Service Path Endpoints (/com.bank.sa.proto.v1.LoanOriginationService/*)
        pathPrefix("com.bank.sa.proto.v1.LoanOriginationService") {
          concat(
            path("EvaluateLoan") {
              post {
                entity(as[LoanOriginationRequest]) { request =>
                  complete(grpcService.evaluateLoan(request))
                }
              }
            },
            path("GetCustomerSituation" / Segment) { customerId =>
              get {
                complete(grpcService.getCustomerSituation(customerId, Instant.now()))
              }
            }
          )
        }
      )
    }

  private def runDemoStep(stepName: String): Future[String] =
    val now = Instant.now()
    stepName match
      case "step0-reset" =>
        gateway.resetAll().map(_ => "Workbench state reset: all active CustomerAgent shards and telemetry logs cleared.")

      case "step1-bootstrap" =>
        val docs = Seq(
          VerifiedDocumentFact(
            docId = "DOC-PAYSLIP-ALICE-01",
            customerId = "CUST-ALICE-001",
            factType = DocumentFactType.VerifiedMonthlyNetIncome,
            numericValue = Some(BigDecimal("4200.00")),
            textValue = Some("Permanent Software Architect"),
            issuer = "National Tax & Payroll Registry",
            verifiedAt = now.minus(5, ChronoUnit.DAYS)
          ),
          VerifiedDocumentFact(
            docId = "DOC-PAYSLIP-BOB-01",
            customerId = "CUST-BOB-002",
            factType = DocumentFactType.VerifiedMonthlyNetIncome,
            numericValue = Some(BigDecimal("3500.00")),
            textValue = Some("Full-Time Consultant"),
            issuer = "National Tax & Payroll Registry",
            verifiedAt = now.minus(7, ChronoUnit.DAYS)
          )
        )
        val rawTxs = Seq(
          RawTransaction(
            txId = "TX-SAL-ALICE-01",
            fromCustomerId = "TECH-EMPLOYER-AG",
            toCounterpartyId = "CUST-ALICE-001",
            counterpartyIsBankClient = true,
            amount = BigDecimal("4200.00"),
            currency = "EUR",
            category = TransactionCategory.Salary,
            resultsInOverdraft = false,
            timestamp = now.minus(4, ChronoUnit.DAYS)
          ),
          RawTransaction(
            txId = "TX-RENT-ALICE-01",
            fromCustomerId = "CUST-ALICE-001",
            toCounterpartyId = "METRO-HOUSING-LTD",
            counterpartyIsBankClient = false,
            amount = BigDecimal("1100.00"),
            currency = "EUR",
            category = TransactionCategory.Housing,
            resultsInOverdraft = false,
            timestamp = now.minus(3, ChronoUnit.DAYS)
          ),
          RawTransaction(
            txId = "TX-PEER-ALICE-BOB-101",
            fromCustomerId = "CUST-ALICE-001",
            toCounterpartyId = "CUST-BOB-002",
            counterpartyIsBankClient = true,
            amount = BigDecimal("500.00"),
            currency = "EUR",
            category = TransactionCategory.PeerTransfer,
            resultsInOverdraft = false,
            timestamp = now.minus(2, ChronoUnit.DAYS)
          )
        )
        val enriched = EnrichmentLogic.enrichBatchInMemory(rawTxs, watchlistProvider())
        gateway.recordTrace(
          stage = "FLINK_ENRICHMENT",
          fromComponent = "Kafka(tx.raw.v1)",
          toComponent = "Flink(BilateralSplitter)",
          eventType = "BatchEnrichment",
          summary = s"Bootstrapped ${docs.size} verified documents and ${enriched.size} bilateral transaction events for Alice & Bob"
        )
        for
          _ <- Future.traverse(docs)(gateway.ingestDocumentFact)
          _ <- Future.traverse(enriched)(gateway.ingestTransaction)
        yield "Step 1 complete: Alice and Bob profiles hydrated with verified payslips and bilateral peer link (TX-PEER-ALICE-BOB-101)."

      case "step2-alice-healthy-loan" =>
        val app = LoanApplication(
          applicationId = "LOAN-ALICE-HEALTHY-01",
          customerId = "CUST-ALICE-001",
          requestedAmount = BigDecimal("12000.00"),
          termMonths = 24,
          annualInterestRate = BigDecimal("0.065"),
          requestedAt = now
        )
        gateway.evaluateLoan(app).map { res =>
          s"Step 2 complete: Evaluated ${app.applicationId} for Alice -> Score ${res.auditTrail.finalCreditScore}/850."
        }

      case "step3-bob-sanction-contagion" =>
        val rawTx = RawTransaction(
          txId = "TX-SANCTION-BOB-999",
          fromCustomerId = "CUST-BOB-002",
          toCounterpartyId = "ENTITY-SHADOW-LTD",
          counterpartyIsBankClient = false,
          amount = BigDecimal("4800.00"),
          currency = "EUR",
          category = TransactionCategory.HighRiskTransfer,
          resultsInOverdraft = false,
          timestamp = now
        )
        val enriched = EnrichmentLogic.enrichBatchInMemory(Seq(rawTx), watchlistProvider())
        gateway.recordTrace(
          stage = "FLINK_ENRICHMENT",
          fromComponent = "Flink(WatchlistBroadcastEnricher)",
          toComponent = "Kafka(customer.situation.events.v1)",
          eventType = "1st-Hop Watchlist Match",
          summary = "Tagged TX-SANCTION-BOB-999 with RestrictedEntityTag(ENTITY-SHADOW-LTD, SanctionedEntity, weight=1.00)"
        )
        for
          _ <- Future.traverse(enriched)(gateway.ingestTransaction)
          bobLoan = LoanApplication("LOAN-BOB-DENIED-01", "CUST-BOB-002", BigDecimal("10000.00"), 24, BigDecimal("0.065"), now)
          _ <- gateway.evaluateLoan(bobLoan)
        yield "Step 3 complete: Bob transacted with ENTITY-SHADOW-LTD -> 1st-hop hit on Bob AND 2nd-hop PeerRiskSignal propagated to Alice!"

      case "step4-alice-distress-loan" =>
        val distressTxs = Seq(
          RawTransaction(
            txId = "TX-GAMB-ALICE-201",
            fromCustomerId = "CUST-ALICE-001",
            toCounterpartyId = "CASINO-ROYALE-BET",
            counterpartyIsBankClient = false,
            amount = BigDecimal("600.00"),
            currency = "EUR",
            category = TransactionCategory.Gambling,
            resultsInOverdraft = true,
            timestamp = now.minus(10, ChronoUnit.MINUTES)
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
            timestamp = now.minus(5, ChronoUnit.MINUTES)
          )
        )
        val enriched = EnrichmentLogic.enrichBatchInMemory(distressTxs, watchlistProvider())
        val aliceLoan2 = LoanApplication(
          applicationId = "LOAN-ALICE-CONTAGION-02",
          customerId = "CUST-ALICE-001",
          requestedAmount = BigDecimal("18000.00"),
          termMonths = 24,
          annualInterestRate = BigDecimal("0.065"),
          requestedAt = now
        )
        for
          _ <- Future.traverse(enriched)(gateway.ingestTransaction)
          _ <- gateway.evaluateLoan(aliceLoan2)
        yield "Step 4 complete: Evaluated Alice's second loan -> DENIED with 2nd-hop peer contagion, high-risk spend velocity, and overdraft reason codes."

      case other =>
        Future.failed(new IllegalArgumentException(s"Unknown demo scenario step: $other"))

  def start(host: String = "127.0.0.1", port: Int = 8088): Future[Http.ServerBinding] =
    Http().newServerAt(host, port).bind(routes)

object LoanOriginationServer:
  val defaultWatchlist: Map[String, RestrictedEntityTag] = Map(
    "ENTITY-SHADOW-LTD" -> RestrictedEntityTag(
      entityId = "ENTITY-SHADOW-LTD",
      level = RestrictionLevel.SanctionedEntity,
      listSource = "EU-AML-SANCTIONS-2026",
      reasonDescription = "Sanctioned shell company linked tos illicit financial flows"
    ),
    "UNLICENSED-LENDER-88" -> RestrictedEntityTag(
      entityId = "UNLICENSED-LENDER-88",
      level = RestrictionLevel.UnlicensedLender,
      listSource = "NATIONAL-REGULATOR-WATCHLIST",
      reasonDescription = "Unlicensed predatory credit provider"
    ),
    "DARKNET-EXCHANGE-X" -> RestrictedEntityTag(
      entityId = "DARKNET-EXCHANGE-X",
      level = RestrictionLevel.FraudWatchlist,
      listSource = "GLOBAL-FRAUD-INTELLIGENCE",
      reasonDescription = "High-risk mixer and fraud ring settlement account"
    )
  )
