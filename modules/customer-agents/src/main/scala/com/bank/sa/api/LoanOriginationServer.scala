package com.bank.sa.api

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.domain.*
import com.bank.sa.enrichment.EnrichmentLogic
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpMethods, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.model.headers.CacheDirectives.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Pekko HTTP & gRPC Gateway Layer for Real-Time Loan Origination, Level-3 Situation Projection,
 * Oracle Exadata 6-Schema & Global Graph Inspection, Hierarchical Agents, and Interactive Business UI.
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

        // 0. Interactive Business & Agent Inspector UI
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
            // GET /api/v1/system/overview — Full cluster, agent, plugins, Oracle schemas, and workflow trace inspector
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
                      recentDecisions = gateway.getRecentDecisions,
                      registeredEventPlugins = gateway.pluginRegistry.registeredPluginIds,
                      oracleSchemasStatus = gateway.oracleRepository.getSchemaRecordCounts,
                      globalPatternsDetected = gateway.oracleRepository.getRiskIntelligenceEvents
                    )
                    complete(StatusCodes.OK, overview)
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                }
              }
            },

            // GET /api/v1/oracle/overview — Inspect all 6 Oracle Exadata Schemas (Section 2, 3, 4, 5, 11)
            path("oracle" / "overview") {
              get {
                val repo = gateway.oracleRepository
                val dto = OracleExadataOverviewDto(
                  schemasRecordCounts = repo.getSchemaRecordCounts,
                  journalEntries = repo.getJournalEntries(50),
                  boundedSnapshots = repo.getSnapshots,
                  pendingOutbox = repo.getPendingOutbox,
                  transactionFacts = repo.getTransactionFacts(50),
                  verifiedDocumentFacts = repo.getVerifiedDocumentFacts,
                  graphVertices = repo.getGraphVertices,
                  graphEdges = repo.getGraphEdges,
                  riskIntelligenceEvents = repo.getRiskIntelligenceEvents,
                  governancePolicies = repo.getGovernancePolicies
                )
                complete(StatusCodes.OK, dto)
              }
            },

            // POST /api/v1/oracle/graph/analyze — Execute Oracle Exadata Global Property Graph Analysis (Section 5 & 14)
            path("oracle" / "graph" / "analyze") {
              post {
                onComplete(gateway.runGlobalGraphAnalysis(Instant.now())) {
                  case Success(patterns) => complete(StatusCodes.OK, patterns)
                  case Failure(ex)       => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                }
              }
            },

            // GET /api/v1/agents/hierarchy — Hierarchical Situation Awareness across all 5 Agent Types (Section 6 & 15)
            path("agents" / "hierarchy") {
              get {
                onComplete(gateway.getGlobalBankingSituation(Instant.now())) {
                  case Success(globalSituation) => complete(StatusCodes.OK, globalSituation)
                  case Failure(ex)              => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
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

            // POST /api/v1/loans/project — Level-3 Situation Projection (independent of formal approval, Section 7)
            path("loans" / "project") {
              post {
                entity(as[LoanOriginationRequest]) { request =>
                  onComplete(grpcService.projectLoan(request)) {
                    case Success(projectionDto) => complete(StatusCodes.OK, projectionDto)
                    case Failure(ex)            => complete(StatusCodes.InternalServerError, Map("error" -> ex.getMessage))
                  }
                }
              }
            },

            // POST /api/v1/loans/evaluate — Formal Explainable Credit Decision
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
                    toComponent = "Flink(Validator + ExternalCategorizer + BilateralSplitter + WatchlistBroadcast)",
                    eventType = if enrichedList.size > 1 then "BilateralSplit (2 Perspectives)" else "SinglePerspective",
                    summary =
                      s"Flink validated, categorized via '${EnrichmentLogic.DefaultCategorizer.categorizerName}', and enriched raw tx '${rawDomain.txId}' (${rawDomain.fromCustomerId} -> ${rawDomain.toCounterpartyId}, ${rawDomain.amount} EUR) into ${enrichedList.size} sharded event(s)",
                    details = Map(
                      "txId" -> rawDomain.txId,
                      "categorizer" -> EnrichmentLogic.DefaultCategorizer.categorizerName,
                      "assignedCategory" -> enrichedList.headOption.map(_.category.toString).getOrElse(rawDomain.category.toString),
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
            path("ProjectLoan") {
              post {
                entity(as[LoanOriginationRequest]) { request =>
                  complete(grpcService.projectLoan(request))
                }
              }
            },
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
        gateway.resetAll().map(_ => "Workbench state reset: all 5 sharded agent types, Oracle Exadata schemas, and telemetry logs cleared.")

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
          toComponent = "Flink(ExternalCategorizer + BilateralSplitter)",
          eventType = "BatchEnrichment",
          summary = s"Bootstrapped ${docs.size} verified documents via DocumentVerificationAgent and ${enriched.size} bilateral transaction events for Alice & Bob"
        )
        for
          _ <- Future.traverse(docs)(gateway.ingestDocumentFact)
          _ <- Future.traverse(enriched)(gateway.ingestTransaction)
        yield "Step 1 complete: Alice and Bob profiles hydrated via DocumentVerificationAgent, EventPluginRegistry, RelationshipAgent, and Oracle Exadata SA_FACTS."

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
          s"Step 2 complete: Evaluated ${app.applicationId} for Alice -> Score ${res.auditTrail.finalCreditScore}/850 (State v${res.auditTrail.stateVersion})."
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
        yield "Step 3 complete: Bob transacted with ENTITY-SHADOW-LTD -> 1st-hop hit on Bob AND reliable 2nd-hop PeerRiskSignal delivered via RiskSignalAgent & Outbox to Alice!"

      case "step4-alice-distress-loan" =>
        val distressTxs = Seq(
          RawTransaction(
            txId = "TX-GAMB-ALICE-201",
            fromCustomerId = "CUST-ALICE-001",
            toCounterpartyId = "CASINO-ROYALE-BET",
            counterpartyIsBankClient = false,
            amount = BigDecimal("600.00"),
            currency = "EUR",
            category = TransactionCategory.EverydaySpend, // ExternalCategorizerModule automatically re-categorizes CASINO-ROYALE-BET to Gambling!
            resultsInOverdraft = true,
            timestamp = now.minus(10, ChronoUnit.MINUTES),
            merchantDescription = Some("CASINO ROYALE BETTING TERMINAL")
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
            timestamp = now.minus(5, ChronoUnit.MINUTES),
            merchantDescription = Some("CASINO ROYALE BETTING TERMINAL")
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
        yield "Step 4 complete: Evaluated Alice's second loan -> DENIED with 2nd-hop peer contagion, external-categorized gambling velocity, and overdraft reason codes."

      case "step5-global-graph-intelligence" =>
        val charlieDoc = VerifiedDocumentFact(
          docId = "DOC-PAYSLIP-CHARLIE-01",
          customerId = "CUST-CHARLIE-003",
          factType = DocumentFactType.VerifiedMonthlyNetIncome,
          numericValue = Some(BigDecimal("4500.00")),
          textValue = Some("Senior Analyst"),
          issuer = "National Tax & Payroll Registry",
          verifiedAt = now.minus(3, ChronoUnit.DAYS)
        )
        val charlieTxs = Seq(
          RawTransaction(
            txId = "TX-SAL-CHARLIE-01",
            fromCustomerId = "GLOBAL-CORP-PAYROLL",
            toCounterpartyId = "CUST-CHARLIE-003",
            counterpartyIsBankClient = true,
            amount = BigDecimal("4500.00"),
            currency = "EUR",
            category = TransactionCategory.Salary,
            resultsInOverdraft = false,
            timestamp = now.minus(2, ChronoUnit.DAYS)
          ),
          RawTransaction(
            txId = "TX-SHADOW-CHARLIE-888",
            fromCustomerId = "CUST-CHARLIE-003",
            toCounterpartyId = "CASINO-ROYALE-BET",
            counterpartyIsBankClient = false,
            amount = BigDecimal("500.00"),
            currency = "EUR",
            category = TransactionCategory.EverydaySpend,
            resultsInOverdraft = false,
            timestamp = now.minus(1, ChronoUnit.MINUTES),
            merchantDescription = Some("CASINO ROYALE BET")
          )
        )
        val enriched = EnrichmentLogic.enrichBatchInMemory(charlieTxs, watchlistProvider())
        for
          _        <- gateway.ingestDocumentFact(charlieDoc)
          _        <- Future.traverse(enriched)(gateway.ingestTransaction)
          patterns <- gateway.runGlobalGraphAnalysis(now)
        yield s"Step 5 complete: Oracle Exadata Global Graph (SA_GRAPH) detected ${patterns.size} multi-account risk pattern(s) and emitted NetworkRiskPatternDetected to CustomerAgents & PortfolioRiskAgent!"

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
      reasonDescription = "Sanctioned shell company linked to illicit financial flows"
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
