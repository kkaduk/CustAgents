package com.bank.sa.agent

import com.bank.sa.api.{LoanOriginationDecisionDto, WorkflowTraceEventDto}
import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import com.bank.sa.oracle.OracleExadataRepository
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import org.apache.pekko.util.Timeout

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
 * Coordinates Pekko Cluster Sharding across all 5 Domain Agent types (Section 6 of Doc/ExaPekdoDesign.md):
 * 1. `CustomerAgent`             — Current situation of an individual customer
 * 2. `RelationshipAgent`         — State of a specific bilateral relationship or exposure
 * 3. `RiskSignalAgent`           — Interpretation, deduplication, and reliable delivery lifecycle of risk signals
 * 4. `DocumentVerificationAgent` — Coordination of external document verification
 * 5. `PortfolioRiskAgent`        — Segment and portfolio-level risk interpretation
 *
 * Also coordinates the `EventPluginRegistry` (Section 2.1), `TransactionalOutbox` reliable delivery (Section 10),
 * and asynchronous projections to the 6 Oracle Exadata Schemas (`OracleExadataRepository`, Sections 2–5, 11, 14).
 */
class CustomerClusterGateway(
    system: ActorSystem[_],
    creditEngine: ExplainableCreditEngine,
    val oracleRepository: OracleExadataRepository = new OracleExadataRepository(),
    val pluginRegistry: EventPluginRegistry = EventPluginRegistry.default
):
  private given Timeout          = Timeout(5.seconds)
  private given ExecutionContext = system.executionContext
  private val sharding           = ClusterSharding(system)

  private val seqCounter            = new AtomicLong(0L)
  private val knownCustomers        = new ConcurrentSkipListSet[String]()
  private val knownRelationships    = new ConcurrentSkipListSet[String]()
  private val knownSegments         = new ConcurrentSkipListSet[String]()
  private val knownSubSegments      = new ConcurrentSkipListSet[String]()
  private val knownRiskCoordinators = new ConcurrentSkipListSet[String]()
  private val workflowTraces        = new ConcurrentLinkedDeque[WorkflowTraceEventDto]()
  private val recentDecisions       = new ConcurrentLinkedDeque[LoanOriginationDecisionDto]()

  knownSegments.add("RETAIL-PRIME")
  knownRiskCoordinators.add(RiskSignalAgentState.coordinatorKeyFor("BOOTSTRAP"))

  def recordTrace(
      stage: String,
      fromComponent: String,
      toComponent: String,
      eventType: String,
      summary: String,
      details: Map[String, String] = Map.empty,
      timestamp: Instant = Instant.now()
  ): Unit =
    val event = WorkflowTraceEventDto(
      sequenceNo = seqCounter.incrementAndGet(),
      stage = stage,
      fromComponent = fromComponent,
      toComponent = toComponent,
      eventType = eventType,
      summary = summary,
      details = details,
      timestamp = timestamp
    )
    workflowTraces.addFirst(event)
    while workflowTraces.size() > 200 do workflowTraces.removeLast()

  private val shardedRiskRouter: PeerRiskRouter = new PeerRiskRouter:
    override def sendRiskSignal(targetCustomerId: String, signal: PeerRiskSignal): Unit =
      knownCustomers.add(targetCustomerId)
      recordTrace(
        stage = "AGENT_TO_AGENT_IPC",
        fromComponent = s"RiskSignalAgent(${signal.fromCustomerId})",
        toComponent = s"CustomerAgent($targetCustomerId)",
        eventType = "PropagatePeerRisk (ReliableDelivery Hop=2)",
        summary =
          s"Reliable delivery of 2nd-hop signal '${signal.signalId}' from '${signal.fromCustomerId}' to '$targetCustomerId' (score=${f"${signal.attenuatedRiskScore}%.2f"})",
        details = Map(
          "signalId" -> signal.signalId,
          "originEntityId" -> signal.originCustomerId,
          "hopCount" -> signal.hopCount.toString,
          "attenuatedRiskScore" -> f"${signal.attenuatedRiskScore}%.2f",
          "reasonCode" -> signal.reasonCode,
          "outboxId" -> signal.outboxId.getOrElse("N/A"),
          "rootEvidenceIds" -> signal.rootEvidenceIds.mkString(", ")
        ),
        timestamp = signal.emittedAt
      )
      val targetEntity   = sharding.entityRefFor(CustomerAgent.TypeKey, targetCustomerId)
      val coordinatorKey = RiskSignalAgentState.coordinatorKeyFor(signal.signalId)
      knownRiskCoordinators.add(coordinatorKey)

      targetEntity
        .ask[CustomerAgent.IngestionAck](replyTo => CustomerAgent.PropagatePeerRisk(signal, Some(replyTo)))
        .foreach { _ =>
          val ackTime = Instant.now()
          sharding
            .entityRefFor(RiskSignalAgent.TypeKey, coordinatorKey)
            .tell(RiskSignalAgent.AcknowledgeSignalDelivery(signal.signalId, ackTime, None))

          signal.outboxId.foreach { outboxId =>
            oracleRepository.markOutboxDelivered(outboxId)
            sharding
              .entityRefFor(CustomerAgent.TypeKey, signal.fromCustomerId)
              .tell(CustomerAgent.ConfirmOutboxDelivery(outboxId, None))
          }
        }

    override def retryPendingOutbox(
        customerId: String,
        pendingEntries: Iterable[OutboxEntry]
    ): Unit =
      pendingEntries.foreach { entry =>
        entry.peerSignal match
          case Some(sig) =>
            val coordinatorKey = RiskSignalAgentState.coordinatorKeyFor(sig.signalId)
            knownRiskCoordinators.add(coordinatorKey)
            sharding
              .entityRefFor(RiskSignalAgent.TypeKey, coordinatorKey)
              .tell(RiskSignalAgent.DispatchAndTrackSignal(sig, None))
            sendRiskSignal(sig.targetCustomerId, sig)
          case None if entry.targetType == OutboxTargetType.RelationshipAgent =>
            oracleRepository.markOutboxDelivered(entry.outboxId)
            sharding
              .entityRefFor(CustomerAgent.TypeKey, customerId)
              .tell(CustomerAgent.ConfirmOutboxDelivery(entry.outboxId, None))
          case _ => ()
      }

    override def onEventProcessedByPlugin(
        customerId: String,
        observedEvent: AgentObservedEvent,
        result: PluginProcessingResult
    ): Unit =
      val persistenceId = s"CustomerAgent:$customerId"

      // 1. Record in Oracle Exadata SA_PERSISTENCE (Batched Event Journal + SituationEvaluated + Bounded Snapshot + Transactional Outbox)
      oracleRepository.appendJournalEventsBatch(
        persistenceId = persistenceId,
        events = List(
          (
            observedEvent.eventType,
            s"Plugin=${result.situationEvaluated.handledByPluginId}, eventId=${observedEvent.eventId}",
            observedEvent.occurredAt
          ),
          (
            "SituationEvaluated",
            s"v=${result.updatedState.stateVersion}, status=${result.updatedState.situationSummary.financialSituationStatus}, completeness=${result.updatedState.dataCompleteness.score}",
            observedEvent.occurredAt
          )
        )
      )
      oracleRepository.saveBoundedSnapshot(persistenceId, result.updatedState)
      oracleRepository.persistOutboxEntries(result.outboxEntries)

      recordTrace(
        stage = "EVENT_PLUGIN_ENGINE",
        fromComponent = s"CustomerAgent($customerId)",
        toComponent = result.situationEvaluated.handledByPluginId,
        eventType = s"${observedEvent.eventType} -> SituationEvaluated",
        summary =
          s"Plugin '${result.situationEvaluated.handledByPluginId}' processed '${observedEvent.eventId}' -> Situation v${result.updatedState.stateVersion} (${result.updatedState.situationSummary.financialSituationStatus})",
        details = Map(
          "eventId" -> observedEvent.eventId,
          "eventType" -> observedEvent.eventType,
          "pluginId" -> result.situationEvaluated.handledByPluginId,
          "stateVersion" -> s"v${result.updatedState.stateVersion}",
          "incomeStability" -> result.updatedState.situationSummary.incomeStability.toString,
          "liquidityTrend" -> result.updatedState.situationSummary.liquidityTrend.toString,
          "dataCompleteness" -> s"${result.updatedState.dataCompleteness.score} (${result.updatedState.dataCompleteness.level})"
        ),
        timestamp = observedEvent.occurredAt
      )

      // 2. Project to Sharded PortfolioRiskAgent Sub-Segment (Section 6, 15 & Roadmap 9.2)
      val segId     = result.updatedState.segmentId
      val subSegKey = PortfolioSituationState.subSegmentKeyFor(segId, customerId)
      knownSegments.add(segId)
      knownSubSegments.add(subSegKey)
      sharding
        .entityRefFor(PortfolioRiskAgent.TypeKey, subSegKey)
        .tell(PortfolioRiskAgent.UpdateCustomerInPortfolio(result.updatedState, None))

      // 3. Process Transactional Outbox entries (RelationshipAgent & Sharded RiskSignalAgent)
      observedEvent match
        case TransactionObserved(_, _, tx, _) =>
          val relId = List(customerId, tx.counterpartyId).sorted.mkString("<->")
          knownRelationships.add(relId)
          val relRisk = tx.restrictedTag.map(_.level.baseRiskWeight).getOrElse(0.0)
          sharding
            .entityRefFor(RelationshipAgent.TypeKey, relId)
            .tell(RelationshipAgent.RecordInteraction(tx, relRisk, None))
          result.outboxEntries.filter(_.targetType == OutboxTargetType.RelationshipAgent).foreach { entry =>
            oracleRepository.markOutboxDelivered(entry.outboxId)
            sharding
              .entityRefFor(CustomerAgent.TypeKey, customerId)
              .tell(CustomerAgent.ConfirmOutboxDelivery(entry.outboxId, None))
          }
        case _ => ()

      result.outboxEntries.flatMap(_.peerSignal).foreach { sig =>
        val relId = List(sig.fromCustomerId, sig.targetCustomerId).sorted.mkString("<->")
        knownRelationships.add(relId)
        sharding
          .entityRefFor(RelationshipAgent.TypeKey, relId)
          .tell(RelationshipAgent.RecordPropagatedRisk(sig.attenuatedRiskScore, sig.emittedAt, None))

        val coordinatorKey = RiskSignalAgentState.coordinatorKeyFor(sig.signalId)
        knownRiskCoordinators.add(coordinatorKey)
        sharding
          .entityRefFor(RiskSignalAgent.TypeKey, coordinatorKey)
          .tell(RiskSignalAgent.DispatchAndTrackSignal(sig, None))
      }

  // --- INITIALIZE ALL 5 SHARDED AGENT TYPES ON THIS CLUSTER NODE (Section 6) ---

  // 1. CustomerAgent
  sharding.init(
    Entity(CustomerAgent.TypeKey) { entityContext =>
      CustomerAgent(
        customerId = entityContext.entityId,
        creditEngine = creditEngine,
        peerRiskRouter = shardedRiskRouter,
        pluginRegistry = pluginRegistry
      )
    }.withStopMessage(CustomerAgent.Passivate)
  )

  // 2. RelationshipAgent
  sharding.init(
    Entity(RelationshipAgent.TypeKey) { entityContext =>
      RelationshipAgent(entityContext.entityId)
    }.withStopMessage(RelationshipAgent.Passivate)
  )

  // 3. RiskSignalAgent
  sharding.init(
    Entity(RiskSignalAgent.TypeKey) { entityContext =>
      RiskSignalAgent(
        agentKey = entityContext.entityId,
        deliverToCustomer = (targetId, sig) => shardedRiskRouter.sendRiskSignal(targetId, sig)
      )
    }.withStopMessage(RiskSignalAgent.Passivate)
  )

  // 4. DocumentVerificationAgent
  sharding.init(
    Entity(DocumentVerificationAgent.TypeKey) { entityContext =>
      DocumentVerificationAgent(entityContext.entityId)
    }.withStopMessage(DocumentVerificationAgent.Passivate)
  )

  // 5. PortfolioRiskAgent
  sharding.init(
    Entity(PortfolioRiskAgent.TypeKey) { entityContext =>
      PortfolioRiskAgent(
        segmentId = entityContext.entityId,
        onSnapshotUpdated = portfolioState => oracleRepository.savePortfolioSnapshot(portfolioState)
      )
    }.withStopMessage(PortfolioRiskAgent.Passivate)
  )

  def entityFor(customerId: String): EntityRef[CustomerAgent.Command] =
    knownCustomers.add(customerId)
    sharding.entityRefFor(CustomerAgent.TypeKey, customerId)

  def ingestTransaction(tx: EnrichedTransaction): Future[CustomerAgent.IngestionAck] =
    val shardId = math.abs(tx.customerId.hashCode % 100)
    // Project canonical fact to Oracle Exadata SA_FACTS & SA_GRAPH (Section 4, 5, 12)
    oracleRepository.projectTransactionFact(tx)

    recordTrace(
      stage = "PEKKO_SHARD_ACTOR",
      fromComponent = s"ShardRegion(shard=$shardId)",
      toComponent = s"CustomerAgent(${tx.customerId})",
      eventType = s"TransactionObserved (${tx.direction})",
      summary =
        s"Routed ${tx.direction} transaction '${tx.txId}' (${tx.amount} ${tx.currency}, category=${tx.category}, categorizer=${tx.categorizerSource}) to CustomerAgent(${tx.customerId})" +
          tx.restrictedTag.fold("")(t => s" [1st-Hop Watchlist Match: ${t.entityId}]"),
      details = Map(
        "txId" -> tx.txId,
        "customerId" -> tx.customerId,
        "counterpartyId" -> tx.counterpartyId,
        "direction" -> tx.direction.toString,
        "amount" -> s"${tx.amount} ${tx.currency}",
        "category" -> tx.category.toString,
        "categorizerSource" -> tx.categorizerSource,
        "overdraft" -> tx.resultsInOverdraft.toString,
        "restrictedMatch" -> tx.restrictedTag.map(t => s"${t.entityId} (${t.level})").getOrElse("None")
      ),
      timestamp = tx.timestamp
    )
    entityFor(tx.customerId).ask(replyTo => CustomerAgent.IngestTransaction(tx, replyTo))

  def ingestDocumentFact(fact: VerifiedDocumentFact): Future[CustomerAgent.IngestionAck] =
    val shardId = math.abs(fact.customerId.hashCode % 100)
    knownCustomers.add(fact.customerId)
    // Project document fact to Oracle Exadata SA_FACTS
    oracleRepository.projectDocumentFact(fact)

    val docAgent = sharding.entityRefFor(DocumentVerificationAgent.TypeKey, fact.customerId)
    docAgent
      .ask[AgentObservedEvent](replyTo => DocumentVerificationAgent.VerifyAndClassifyDocument(fact, replyTo))
      .flatMap { observedEvent =>
        recordTrace(
          stage = "PEKKO_SHARD_ACTOR",
          fromComponent = s"DocumentVerificationAgent(${fact.customerId})",
          toComponent = s"CustomerAgent(${fact.customerId}) [Shard $shardId]",
          eventType = s"${observedEvent.eventType} (${fact.factType})",
          summary =
            s"DocumentVerificationAgent verified '${fact.docId}' and emitted '${observedEvent.eventType}' (${fact.factType} = ${fact.numericValue.orElse(fact.textValue).getOrElse("")}) to CustomerAgent(${fact.customerId})",
          details = Map(
            "docId" -> fact.docId,
            "customerId" -> fact.customerId,
            "factType" -> fact.factType.toString,
            "derivedEventType" -> observedEvent.eventType,
            "issuer" -> fact.issuer
          ),
          timestamp = fact.verifiedAt
        )
        entityFor(fact.customerId).ask(replyTo => CustomerAgent.IngestObservedEvent(observedEvent, replyTo))
      }

  /**
   * Level-3 Situation Awareness Projection (Section 7):
   * Calculates future DTI, expected liquidity, risk exposure, and affordability without executing a formal approval/denial.
   */
  def projectLoanSituation(application: LoanApplication): Future[SituationProjection] =
    entityFor(application.customerId)
      .ask[SituationProjection](replyTo => CustomerAgent.ProjectLoanSituation(application, replyTo))
      .map { projection =>
        oracleRepository.recordProjectionAudit(projection)
        recordTrace(
          stage = "SITUATION_PROJECTION",
          fromComponent = s"CustomerAgent(${application.customerId})",
          toComponent = "ExplainableCreditEngine.projectSituation",
          eventType = s"SituationProjection (Affordable=${projection.isAffordable})",
          summary =
            s"Projected scenario for '${application.applicationId}': Future DTI=${(projection.futureDti * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP)}%, Expected Liquidity=${projection.expectedMonthlyLiquidity} EUR, State v${projection.stateVersion}",
          details = Map(
            "applicationId" -> application.applicationId,
            "customerId" -> application.customerId,
            "futureDti" -> s"${(projection.futureDti * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP)}%",
            "expectedLiquidity" -> s"${projection.expectedMonthlyLiquidity} EUR",
            "isAffordable" -> projection.isAffordable.toString,
            "stateVersion" -> s"v${projection.stateVersion}",
            "dataCompleteness" -> s"${projection.dataCompleteness.score} (${projection.dataCompleteness.level})"
          ),
          timestamp = application.requestedAt
        )
        projection
      }

  def evaluateLoan(application: LoanApplication): Future[LoanDecisionResponse] =
    entityFor(application.customerId)
      .ask[LoanDecisionResponse](replyTo => CustomerAgent.EvaluateLoan(application, replyTo))
      .map { response =>
        oracleRepository.recordDecisionAudit(response)
        response.auditTrail.situationProjection.foreach(oracleRepository.recordProjectionAudit)

        val dto = LoanOriginationDecisionDto.fromDomain(response, application.estimatedMonthlyInstallment)
        recentDecisions.addFirst(dto)
        while recentDecisions.size() > 50 do recentDecisions.removeLast()

        recordTrace(
          stage = "EXPLAINABLE_ENGINE",
          fromComponent = s"CustomerAgent(${application.customerId})",
          toComponent = "ExplainableCreditEngine",
          eventType = s"LoanDecisionAudited (${dto.status})",
          summary =
            s"Evaluated loan '${application.applicationId}' (${application.requestedAmount} EUR): ${dto.status} | Score: ${dto.creditScore}/850 | State v${response.auditTrail.stateVersion}" +
              (if dto.adverseActionReasons.nonEmpty then
                 s" | Reasons: ${dto.adverseActionReasons.map(_.code).mkString(", ")}"
               else ""),
          details = Map(
            "applicationId" -> application.applicationId,
            "customerId" -> application.customerId,
            "decision" -> dto.status,
            "creditScore" -> s"${dto.creditScore} / 850",
            "stateVersion" -> s"v${response.auditTrail.stateVersion}",
            "dataCompleteness" -> s"${response.auditTrail.dataCompleteness.score} (${response.auditTrail.dataCompleteness.level})",
            "reasonCodes" -> (if dto.adverseActionReasons.isEmpty then "NONE"
                              else dto.adverseActionReasons.map(_.code).mkString(", "))
          ),
          timestamp = application.requestedAt
        )
        response
      }

  /**
   * Executes the Global Relationship Graph Analysis cycle (Section 5 & Section 14):
   * Local Events -> Local Situation -> Global Analysis (Oracle Exadata SA_GRAPH) ->
   * New Risk Intelligence (`NetworkRiskPatternDetected`) -> Local Situation Update (`CustomerAgent` & `PortfolioRiskAgent`).
   */
  def runGlobalGraphAnalysis(now: Instant = Instant.now()): Future[List[NetworkRiskPatternDetected]] =
    val patterns = oracleRepository.analyzeGlobalGraphAndDetectPatterns(now)
    val propagationFutures = patterns.map { pattern =>
      knownCustomers.add(pattern.affectedCustomerId)
      recordTrace(
        stage = "ORACLE_GLOBAL_GRAPH",
        fromComponent = "OracleExadata(SA_GRAPH -> SA_INTELLIGENCE)",
        toComponent = s"CustomerAgent(${pattern.affectedCustomerId})",
        eventType = s"NetworkRiskPatternDetected (${pattern.patternType})",
        summary =
          s"Oracle Global Graph emitted '${pattern.patternId}' (${pattern.description}, score=${f"${pattern.riskScore}%.2f"}) -> routed to CustomerAgent(${pattern.affectedCustomerId})",
        details = Map(
          "patternId" -> pattern.patternId,
          "patternType" -> pattern.patternType.toString,
          "sharedEntityId" -> pattern.sharedEntityId,
          "linkedCustomers" -> pattern.linkedCustomers.mkString(", "),
          "riskScore" -> f"${pattern.riskScore}%.2f",
          "evidenceIds" -> pattern.evidenceIds.mkString(", ")
        ),
        timestamp = pattern.detectedAt
      )

      val subSegKey = PortfolioSituationState.subSegmentKeyFor("RETAIL-PRIME", pattern.affectedCustomerId)
      knownSubSegments.add(subSegKey)
      sharding
        .entityRefFor(PortfolioRiskAgent.TypeKey, subSegKey)
        .tell(PortfolioRiskAgent.IngestGlobalRiskPattern(pattern, None))

      entityFor(pattern.affectedCustomerId)
        .ask[CustomerAgent.IngestionAck](replyTo => CustomerAgent.IngestNetworkRiskPattern(pattern, Some(replyTo)))
        .map(_ => pattern)
    }
    Future.sequence(propagationFutures)

  def getSituationState(customerId: String): Future[CustomerSituationState] =
    entityFor(customerId).ask(replyTo => CustomerAgent.GetSituationState(replyTo))

  def getRelationshipStates: Future[List[RelationshipSituationState]] =
    val relIds = knownRelationships.asScala.toList
    Future.traverse(relIds) { relId =>
      sharding.entityRefFor(RelationshipAgent.TypeKey, relId).ask(replyTo => RelationshipAgent.GetRelationshipState(replyTo))
    }

  def getRiskSignalAgentState: Future[RiskSignalAgentState] =
    val coordKeys = knownRiskCoordinators.asScala.toList
    Future
      .traverse(coordKeys) { key =>
        sharding
          .entityRefFor(RiskSignalAgent.TypeKey, key)
          .ask[RiskSignalAgentState](replyTo => RiskSignalAgent.GetRiskSignalState(replyTo))
      }
      .map(shards => RiskSignalAgentState.mergeShards("SHARDED-RISK-SIGNAL-COORDINATORS", shards))

  def getPortfolioStates: Future[List[PortfolioSituationState]] =
    val segs    = knownSegments.asScala.toList
    val subSegs = knownSubSegments.asScala.toList
    Future
      .traverse(subSegs) { subKey =>
        sharding
          .entityRefFor(PortfolioRiskAgent.TypeKey, subKey)
          .ask[PortfolioSituationState](replyTo => PortfolioRiskAgent.GetPortfolioSituation(replyTo))
      }
      .map { subStates =>
        segs.map { segId =>
          val matching = subStates.filter(_.segmentId.startsWith(s"$segId#"))
          PortfolioSituationState.mergeSubSegments(segId, matching)
        }
      }

  def getDocumentVerificationStates: Future[List[DocumentVerificationAgentState]] =
    val cids = knownCustomers.asScala.toList
    Future.traverse(cids) { cid =>
      sharding
        .entityRefFor(DocumentVerificationAgent.TypeKey, cid)
        .ask(replyTo => DocumentVerificationAgent.GetVerificationState(replyTo))
    }

  def getGlobalBankingSituation(now: Instant = Instant.now()): Future[GlobalBankingSituation] =
    for
      rels       <- getRelationshipStates
      riskState  <- getRiskSignalAgentState
      docStates  <- getDocumentVerificationStates
      portfolios <- getPortfolioStates
    yield GlobalBankingSituation(
      activeCustomersCount = knownCustomers.size(),
      activeRelationshipsCount = rels.size,
      activeRiskSignalsCount = riskState.signals.size,
      verifiedDocumentsCount = oracleRepository.getVerifiedDocumentFacts.size,
      portfolioSegments = portfolios,
      relationships = rels,
      riskSignalCoordinator = Some(riskState),
      documentVerifications = docStates,
      globalGraphVerticesCount = oracleRepository.getGraphVertices.size,
      globalGraphEdgesCount = oracleRepository.getGraphEdges.size,
      detectedGlobalPatterns = oracleRepository.getRiskIntelligenceEvents,
      oracleSchemasStatus = oracleRepository.getSchemaRecordCounts,
      evaluatedAt = now
    )

  def resetAll(): Future[Unit] =
    val currentIds = knownCustomers.asScala.toList
    val relIds     = knownRelationships.asScala.toList
    val subSegIds  = knownSubSegments.asScala.toList
    val riskKeys   = knownRiskCoordinators.asScala.toList

    for
      _ <- Future.traverse(currentIds) { cid =>
        sharding.entityRefFor(CustomerAgent.TypeKey, cid).ask[CustomerAgent.IngestionAck](replyTo => CustomerAgent.ResetState(replyTo))
      }
      _ <- Future.traverse(currentIds) { cid =>
        sharding
          .entityRefFor(DocumentVerificationAgent.TypeKey, cid)
          .ask[CustomerAgent.IngestionAck](replyTo => DocumentVerificationAgent.ResetVerificationState(replyTo))
      }
      _ <- Future.traverse(relIds) { rid =>
        sharding.entityRefFor(RelationshipAgent.TypeKey, rid).ask[CustomerAgent.IngestionAck](replyTo => RelationshipAgent.ResetRelationship(replyTo))
      }
      _ <- Future.traverse(subSegIds) { sid =>
        sharding.entityRefFor(PortfolioRiskAgent.TypeKey, sid).ask[CustomerAgent.IngestionAck](replyTo => PortfolioRiskAgent.ResetPortfolio(replyTo))
      }
      _ <- Future.traverse(riskKeys) { rkey =>
        sharding
          .entityRefFor(RiskSignalAgent.TypeKey, rkey)
          .ask[CustomerAgent.IngestionAck](replyTo => RiskSignalAgent.ResetRiskSignals(replyTo))
      }
    yield
      knownCustomers.clear()
      knownRelationships.clear()
      knownSubSegments.clear()
      knownRiskCoordinators.clear()
      knownRiskCoordinators.add(RiskSignalAgentState.coordinatorKeyFor("BOOTSTRAP"))
      workflowTraces.clear()
      recentDecisions.clear()
      oracleRepository.reset()
      seqCounter.set(0L)

  def getActiveCustomerIds: List[String] =
    knownCustomers.asScala.toList

  def getWorkflowTraces: List[WorkflowTraceEventDto] =
    workflowTraces.asScala.toList

  def getRecentDecisions: List[LoanOriginationDecisionDto] =
    recentDecisions.asScala.toList
