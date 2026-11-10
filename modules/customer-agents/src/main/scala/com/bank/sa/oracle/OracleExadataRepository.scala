package com.bank.sa.oracle

import com.bank.sa.domain.*

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

/**
 * Represents a row in `SA_PERSISTENCE.AGENT_EVENT_JOURNAL` (Section 2.1 & 11).
 */
final case class OracleJournalRow(
    persistenceId: String,
    sequenceNr: Long,
    eventType: String,
    eventPayloadSummary: String,
    eventTimestamp: Instant,
    eventSchemaVersion: Int = 1
) extends Serializable

/**
 * Represents a bounded snapshot row in `SA_PERSISTENCE.AGENT_STATE_SNAPSHOTS` (Section 3 & 11).
 */
final case class OracleSnapshotRow(
    persistenceId: String,
    sequenceNr: Long,
    stateVersion: Long,
    ruleVersion: String,
    monthlyIncome: BigDecimal,
    incomeStability: String,
    liquidityTrend: String,
    completenessScore: Double,
    activeExposuresCount: Int,
    snapshottedAt: Instant
) extends Serializable

/**
 * Represents a canonical financial transaction fact in `SA_FACTS.FINANCIAL_TRANSACTION_FACTS` (Section 4 & 11).
 */
final case class FinancialTransactionFactRow(
    txId: String,
    customerId: String,
    counterpartyId: String,
    counterpartyIsBankClient: Boolean,
    direction: String,
    amount: BigDecimal,
    currency: String,
    category: String,
    categorizerSource: String,
    resultsInOverdraft: Boolean,
    restrictedEntityId: Option[String],
    occurredAt: Instant,
    projectedAt: Instant
) extends Serializable

/**
 * Represents a governance policy entry in `SA_GOVERNANCE.GOVERNANCE_POLICY_REGISTRY` (Section 11 & 13).
 */
final case class GovernancePolicyRow(
    policyVersion: String,
    modelName: String,
    maxDtiRatio: BigDecimal,
    maxHighRiskSpendRatio: BigDecimal,
    maxOverdraftCount: Int,
    max1HopRiskScore: Double,
    max2HopRiskScore: Double,
    active: Boolean,
    registeredAt: Instant
) extends Serializable

/**
 * Implements the Oracle Exadata 6-Schema Global Memory, Projection Store, and Global Property Graph Analyzer
 * (`SA_PERSISTENCE`, `SA_FACTS`, `SA_GRAPH`, `SA_INTELLIGENCE`, `SA_DECISIONS`, `SA_GOVERNANCE`)
 * as specified in Sections 2, 3, 4, 5, 11, 12, and 14 of `Doc/ExaPekdoDesign.md`.
 *
 * Works seamlessly in-memory / embedded mode for deterministic sub-second tests and local execution,
 * while mirroring the exact schema separation defined in `sql/oracle_exadata_schemas.sql` for Oracle Exadata JDBC.
 */
class OracleExadataRepository:

  private val seqCounters         = new ConcurrentHashMap[String, AtomicLong]()
  // 1. SA_PERSISTENCE
  private val persistenceJournal  = new ConcurrentLinkedDeque[OracleJournalRow]()
  private val persistenceSnapshots = new ConcurrentHashMap[String, OracleSnapshotRow]()
  private val persistenceOutbox   = new ConcurrentHashMap[String, OutboxEntry]()

  // 2. SA_FACTS
  private val financialTxFacts    = new ConcurrentLinkedDeque[FinancialTransactionFactRow]()
  private val verifiedDocFacts    = new ConcurrentHashMap[String, VerifiedDocumentFact]()
  private val debtObligationFacts = new ConcurrentHashMap[String, VerifiedDocumentFact]()

  // 3. SA_GRAPH (Oracle Property Graph: Vertices & Edges)
  private val graphVertices       = new ConcurrentHashMap[String, GraphVertex]()
  private val graphEdges          = new ConcurrentHashMap[String, GraphEdge]()

  // 4. SA_INTELLIGENCE (Global Risk Intelligence & Portfolio Analytics)
  private val riskIntelEvents     = new ConcurrentLinkedDeque[NetworkRiskPatternDetected]()
  private val portfolioSnapshots  = new ConcurrentHashMap[String, PortfolioSituationState]()

  // 5. SA_DECISIONS (Persisted Evaluations & Projections)
  private val decisionAuditLog    = new ConcurrentLinkedDeque[LoanDecisionResponse]()
  private val projectionAuditLog  = new ConcurrentLinkedDeque[SituationProjection]()

  // 6. SA_GOVERNANCE (Rule Versions, Models, Policies)
  private val governancePolicies  = new ConcurrentHashMap[String, GovernancePolicyRow]()

  // Bootstrap default governance policy
  registerGovernancePolicy(
    GovernancePolicyRow(
      policyVersion = "SA-CREDIT-POLICY-2026.11",
      modelName = "Endsley-SA-Hybrid-Scorecard-v2",
      maxDtiRatio = BigDecimal("0.40"),
      maxHighRiskSpendRatio = BigDecimal("0.15"),
      maxOverdraftCount = 1,
      max1HopRiskScore = 0.50,
      max2HopRiskScore = 0.45,
      active = true,
      registeredAt = Instant.parse("2026-11-01T00:00:00Z")
    )
  )

  private def nextSeq(persistenceId: String): Long =
    seqCounters.computeIfAbsent(persistenceId, _ => new AtomicLong(1000L)).incrementAndGet()

  // --- 1. SA_PERSISTENCE OPERATIONS ---

  def appendJournalEvent(
      persistenceId: String,
      eventType: String,
      payloadSummary: String,
      timestamp: Instant = Instant.now()
  ): OracleJournalRow =
    val seq = nextSeq(persistenceId)
    val row = OracleJournalRow(
      persistenceId = persistenceId,
      sequenceNr = seq,
      eventType = eventType,
      eventPayloadSummary = payloadSummary,
      eventTimestamp = timestamp,
      eventSchemaVersion = 1
    )
    persistenceJournal.addFirst(row)
    while persistenceJournal.size() > 500 do persistenceJournal.removeLast()
    row

  def saveBoundedSnapshot(persistenceId: String, state: CustomerSituationState): OracleSnapshotRow =
    val seq = seqCounters.getOrDefault(persistenceId, new AtomicLong(1000L)).get()
    val snap = OracleSnapshotRow(
      persistenceId = persistenceId,
      sequenceNr = seq,
      stateVersion = state.stateVersion,
      ruleVersion = state.ruleVersion,
      monthlyIncome = state.situationSummary.monthlyIncome,
      incomeStability = state.situationSummary.incomeStability.toString,
      liquidityTrend = state.situationSummary.liquidityTrend.toString,
      completenessScore = state.dataCompleteness.score,
      activeExposuresCount = state.riskExposures.size,
      snapshottedAt = Instant.now()
    )
    persistenceSnapshots.put(persistenceId, snap)
    snap

  def persistOutboxEntries(entries: Iterable[OutboxEntry]): Unit =
    entries.foreach(e => persistenceOutbox.put(e.outboxId, e))

  def markOutboxDelivered(outboxId: String): Unit =
    persistenceOutbox.remove(outboxId)

  // --- 2. SA_FACTS PROJECTION OPERATIONS ---

  def projectTransactionFact(tx: EnrichedTransaction): FinancialTransactionFactRow =
    val row = FinancialTransactionFactRow(
      txId = tx.txId,
      customerId = tx.customerId,
      counterpartyId = tx.counterpartyId,
      counterpartyIsBankClient = tx.counterpartyIsBankClient,
      direction = tx.direction.toString,
      amount = tx.amount,
      currency = tx.currency,
      category = tx.category.toString,
      categorizerSource = tx.categorizerSource,
      resultsInOverdraft = tx.resultsInOverdraft,
      restrictedEntityId = tx.restrictedTag.map(_.entityId),
      occurredAt = tx.timestamp,
      projectedAt = Instant.now()
    )
    financialTxFacts.addFirst(row)
    while financialTxFacts.size() > 500 do financialTxFacts.removeLast()
    projectToGlobalPropertyGraph(tx)
    row

  def projectDocumentFact(fact: VerifiedDocumentFact): Unit =
    verifiedDocFacts.put(fact.docId, fact)
    if fact.factType == DocumentFactType.ExternalDebtObligation then
      debtObligationFacts.put(fact.docId, fact)

  // --- 3. SA_GRAPH (ORACLE PROPERTY GRAPH) PROJECTION & ANALYSIS ---

  def projectToGlobalPropertyGraph(tx: EnrichedTransaction): Unit =
    val srcId = if tx.direction == TransactionDirection.Outflow then tx.customerId else tx.counterpartyId
    val tgtId = if tx.direction == TransactionDirection.Outflow then tx.counterpartyId else tx.customerId
    val edgeKey = s"$srcId->$tgtId"

    val existingEdge = Option(graphEdges.get(edgeKey))
    val updatedEdge = existingEdge match
      case Some(e) =>
        e.copy(
          totalVolume = e.totalVolume + tx.amount,
          txCount = e.txCount + 1,
          evidenceTxIds = (tx.txId :: e.evidenceTxIds).distinct.take(25),
          lastActiveAt = if tx.timestamp.isAfter(e.lastActiveAt) then tx.timestamp else e.lastActiveAt
        )
      case None =>
        GraphEdge(
          edgeId = edgeKey,
          sourceVertexId = srcId,
          targetVertexId = tgtId,
          relationshipType = if tx.restrictedTag.isDefined then "RESTRICTED_FLOW" else "FINANCIAL_TRANSFER",
          totalVolume = tx.amount,
          txCount = 1,
          evidenceTxIds = List(tx.txId),
          lastActiveAt = tx.timestamp
        )
    graphEdges.put(edgeKey, updatedEdge)

    val customerRisk = tx.restrictedTag.map(_.level.baseRiskWeight * 0.5).getOrElse(0.0)
    upsertVertex(
      vertexId = tx.customerId,
      vertexType = "BANK_CUSTOMER",
      intrinsicRisk = customerRisk,
      at = tx.timestamp
    )

    val cpType =
      if tx.restrictedTag.isDefined then "RESTRICTED_ENTITY"
      else if tx.counterpartyIsBankClient then "BANK_CUSTOMER"
      else "EXTERNAL_ENTITY"
    val cpRisk = tx.restrictedTag.map(_.level.baseRiskWeight).getOrElse(
      if tx.category.isHighRiskCategory then 0.65 else 0.0
    )
    upsertVertex(
      vertexId = tx.counterpartyId,
      vertexType = cpType,
      intrinsicRisk = cpRisk,
      at = tx.timestamp
    )

  private def upsertVertex(
      vertexId: String,
      vertexType: String,
      intrinsicRisk: Double,
      at: Instant
  ): Unit =
    val degree = graphEdges.values().asScala.count(e => e.sourceVertexId == vertexId || e.targetVertexId == vertexId)
    val existing = Option(graphVertices.get(vertexId))
    val updated = existing match
      case Some(v) =>
        v.copy(
          vertexType = if v.vertexType == "RESTRICTED_ENTITY" then v.vertexType else vertexType,
          intrinsicRisk = math.max(v.intrinsicRisk, intrinsicRisk),
          degreeCentrality = degree,
          lastUpdatedAt = if at.isAfter(v.lastUpdatedAt) then at else v.lastUpdatedAt
        )
      case None =>
        GraphVertex(
          vertexId = vertexId,
          vertexType = vertexType,
          segmentId = "RETAIL-PRIME",
          intrinsicRisk = intrinsicRisk,
          degreeCentrality = degree,
          communityId = s"COMM-${math.abs(vertexId.hashCode % 10)}",
          lastUpdatedAt = at
        )
    graphVertices.put(vertexId, updated)

  /**
   * Executes Global Relationship Graph Analysis on Oracle Exadata (`SA_GRAPH` -> `SA_INTELLIGENCE`, Section 5 & 14):
   * Detects:
   * 1. Shared high-risk / external counterparty clusters across multiple seemingly independent bank customers
   *    (e.g., `Alice -> Company X` and `Charlie -> Company X`, or multiple clients using the same gambling/crypto/offshore hub).
   * 2. High-degree hub nodes ("super-nodes") concentrating systemic risk.
   *
   * Emits `NetworkRiskPatternDetected` events to be published via Kafka to Pekko CustomerAgents and PortfolioRiskAgents!
   */
  def analyzeGlobalGraphAndDetectPatterns(now: Instant = Instant.now()): List[NetworkRiskPatternDetected] =
    val allEdges    = graphEdges.values().asScala.toList
    val allVertices = graphVertices.asScala.toMap

    // Group edges by target counterparty to find shared counterparties across multiple bank customers
    val byCounterparty: Map[String, List[GraphEdge]] = allEdges.groupBy(_.targetVertexId)

    val detectedPatterns = byCounterparty.toList.flatMap { case (counterpartyId, incomingEdges) =>
      val distinctCustomers = incomingEdges
        .map(_.sourceVertexId)
        .filter(vid => allVertices.get(vid).exists(_.vertexType == "BANK_CUSTOMER"))
        .distinct

      val cpVertexOpt = allVertices.get(counterpartyId)
      val isRiskyCounterparty = cpVertexOpt.exists(v => v.intrinsicRisk >= 0.50 || v.vertexType == "RESTRICTED_ENTITY")

      // Pattern A: Multiple bank customers (>= 2) transacting with the same high-risk or watchlisted counterparty,
      // OR a high-volume shared non-bank entity with elevated risk
      if distinctCustomers.size >= 2 && (isRiskyCounterparty || incomingEdges.map(_.totalVolume).sum >= BigDecimal("1000.00")) then
        val baseScore = cpVertexOpt.map(_.intrinsicRisk).filter(_ > 0.0).getOrElse(0.55)
        val patternScore = BigDecimal(math.min(0.95, baseScore)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val allEvidence  = incomingEdges.flatMap(_.evidenceTxIds).distinct

        distinctCustomers.map { affectedCust =>
          val pattern = NetworkRiskPatternDetected(
            patternId = s"PAT-GLOBAL-${counterpartyId.replaceAll("[^A-Za-z0-9]", "")}-${affectedCust.replaceAll("[^A-Za-z0-9]", "")}",
            patternType =
              if distinctCustomers.size >= 3 then NetworkRiskPatternType.HighDegreeNodeExposure
              else NetworkRiskPatternType.SharedHighRiskCounterparty,
            affectedCustomerId = affectedCust,
            sharedEntityId = counterpartyId,
            linkedCustomers = distinctCustomers,
            riskScore = patternScore,
            reasonCode = "RC_303_GLOBAL_GRAPH_SHARED_CLUSTER",
            description =
              s"Global Property Graph detected ${distinctCustomers.size} bank accounts (${distinctCustomers.mkString(", ")}) converging on shared counterparty '$counterpartyId'",
            evidenceIds = allEvidence,
            detectedAt = now
          )
          recordRiskIntelligenceEvent(pattern)
          pattern
        }
      else Nil
    }

    detectedPatterns

  // --- 4. SA_INTELLIGENCE OPERATIONS ---

  def recordRiskIntelligenceEvent(event: NetworkRiskPatternDetected): Unit =
    riskIntelEvents.addFirst(event)
    while riskIntelEvents.size() > 200 do riskIntelEvents.removeLast()

  def savePortfolioSnapshot(state: PortfolioSituationState): Unit =
    portfolioSnapshots.put(state.segmentId, state)

  // --- 5. SA_DECISIONS OPERATIONS ---

  def recordProjectionAudit(projection: SituationProjection): Unit =
    projectionAuditLog.addFirst(projection)
    while projectionAuditLog.size() > 200 do projectionAuditLog.removeLast()

  def recordDecisionAudit(decision: LoanDecisionResponse): Unit =
    decisionAuditLog.addFirst(decision)
    while decisionAuditLog.size() > 200 do decisionAuditLog.removeLast()

  // --- 6. SA_GOVERNANCE OPERATIONS ---

  def registerGovernancePolicy(policy: GovernancePolicyRow): Unit =
    governancePolicies.put(policy.policyVersion, policy)

  // --- QUERY & INSPECTION METHODS ---

  def getJournalEntries(limit: Int = 50): List[OracleJournalRow] =
    persistenceJournal.asScala.take(limit).toList

  def getSnapshots: List[OracleSnapshotRow] =
    persistenceSnapshots.values().asScala.toList

  def getPendingOutbox: List[OutboxEntry] =
    persistenceOutbox.values().asScala.toList

  def getTransactionFacts(limit: Int = 50): List[FinancialTransactionFactRow] =
    financialTxFacts.asScala.take(limit).toList

  def getVerifiedDocumentFacts: List[VerifiedDocumentFact] =
    verifiedDocFacts.values().asScala.toList

  def getGraphVertices: List[GraphVertex] =
    graphVertices.values().asScala.toList

  def getGraphEdges: List[GraphEdge] =
    graphEdges.values().asScala.toList

  def getRiskIntelligenceEvents: List[NetworkRiskPatternDetected] =
    riskIntelEvents.asScala.toList

  def getPortfolioSnapshots: List[PortfolioSituationState] =
    portfolioSnapshots.values().asScala.toList

  def getDecisionAuditLog: List[LoanDecisionResponse] =
    decisionAuditLog.asScala.toList

  def getProjectionAuditLog: List[SituationProjection] =
    projectionAuditLog.asScala.toList

  def getGovernancePolicies: List[GovernancePolicyRow] =
    governancePolicies.values().asScala.toList

  def getSchemaRecordCounts: Map[String, Int] = Map(
    "SA_PERSISTENCE"  -> (persistenceJournal.size() + persistenceSnapshots.size() + persistenceOutbox.size()),
    "SA_FACTS"        -> (financialTxFacts.size() + verifiedDocFacts.size() + debtObligationFacts.size()),
    "SA_GRAPH"        -> (graphVertices.size() + graphEdges.size()),
    "SA_INTELLIGENCE" -> (riskIntelEvents.size() + portfolioSnapshots.size()),
    "SA_DECISIONS"    -> (decisionAuditLog.size() + projectionAuditLog.size()),
    "SA_GOVERNANCE"   -> governancePolicies.size()
  )

  def reset(): Unit =
    seqCounters.clear()
    persistenceJournal.clear()
    persistenceSnapshots.clear()
    persistenceOutbox.clear()
    financialTxFacts.clear()
    verifiedDocFacts.clear()
    debtObligationFacts.clear()
    graphVertices.clear()
    graphEdges.clear()
    riskIntelEvents.clear()
    portfolioSnapshots.clear()
    decisionAuditLog.clear()
    projectionAuditLog.clear()

