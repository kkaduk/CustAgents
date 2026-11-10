package com.bank.sa.domain

import java.time.Instant

// =============================================================================
// SECTION 6 & 15: DOMAIN AGENT STATES & HIERARCHICAL SITUATION AWARENESS
// Customer Situation -> Relationship Situation -> Portfolio Situation -> Global Banking Situation
// =============================================================================

/**
 * State maintained by `RelationshipAgent` (Section 6 & 15):
 * Represents the state of a specific relationship or bilateral exposure between two parties (`entityA <-> entityB`).
 */
final case class RelationshipSituationState(
    relationshipId: String,
    partyA: String,
    partyB: String,
    bothAreBankClients: Boolean = false,
    totalVolume: BigDecimal = BigDecimal(0),
    transactionCount: Int = 0,
    sharedRiskScore: Double = 0.0,
    propagatedSignalsCount: Int = 0,
    recentTxIds: List[String] = Nil,
    lastInteractionAt: Instant = Instant.EPOCH
) extends Serializable:

  def recordInteraction(tx: EnrichedTransaction, riskScore: Double): RelationshipSituationState =
    val updatedTime = if tx.timestamp.isAfter(lastInteractionAt) then tx.timestamp else lastInteractionAt
    copy(
      bothAreBankClients = bothAreBankClients || tx.counterpartyIsBankClient,
      totalVolume = totalVolume + tx.amount,
      transactionCount = transactionCount + 1,
      sharedRiskScore = math.max(sharedRiskScore, riskScore),
      recentTxIds = (tx.txId :: recentTxIds).distinct.take(25),
      lastInteractionAt = updatedTime
    )

  def recordRiskPropagation(signalScore: Double, at: Instant): RelationshipSituationState =
    copy(
      sharedRiskScore = math.max(sharedRiskScore, signalScore),
      propagatedSignalsCount = propagatedSignalsCount + 1,
      lastInteractionAt = if at.isAfter(lastInteractionAt) then at else lastInteractionAt
    )

/**
 * Lifecycle status of a risk signal managed by `RiskSignalAgent` (Section 6 & 10).
 */
enum RiskSignalStatus:
  case Emitted, DeliveredAndAcknowledged, SuppressedDuplicate, Expired

final case class RiskSignalLifecycleRecord(
    signalId: String,
    originEntityId: String,
    fromCustomerId: String,
    targetCustomerId: String,
    hopCount: Int,
    attenuatedRiskScore: Double,
    status: RiskSignalStatus,
    outboxId: Option[String],
    emittedAt: Instant,
    acknowledgedAt: Option[Instant] = None
) extends Serializable

/**
 * State maintained by `RiskSignalAgent` (Section 6):
 * Coordinates the interpretation, deduplication, attenuation, and reliable delivery lifecycle of risk signals.
 */
final case class RiskSignalAgentState(
    agentKey: String,
    signals: Map[String, RiskSignalLifecycleRecord] = Map.empty,
    deliveredCount: Int = 0,
    deduplicatedCount: Int = 0,
    lastUpdatedAt: Instant = Instant.EPOCH
) extends Serializable:

  def hasSeenSignal(signalId: String): Boolean = signals.contains(signalId)

  def registerSignal(signal: PeerRiskSignal): RiskSignalAgentState =
    if signals.contains(signal.signalId) then
      copy(deduplicatedCount = deduplicatedCount + 1)
    else
      val record = RiskSignalLifecycleRecord(
        signalId = signal.signalId,
        originEntityId = signal.originCustomerId,
        fromCustomerId = signal.fromCustomerId,
        targetCustomerId = signal.targetCustomerId,
        hopCount = signal.hopCount,
        attenuatedRiskScore = signal.attenuatedRiskScore,
        status = RiskSignalStatus.Emitted,
        outboxId = signal.outboxId,
        emittedAt = signal.emittedAt
      )
      copy(
        signals = signals.updated(signal.signalId, record),
        lastUpdatedAt = signal.emittedAt
      )

  def markDelivered(signalId: String, ackTime: Instant): RiskSignalAgentState =
    signals.get(signalId) match
      case Some(existing) =>
        val updated = existing.copy(
          status = RiskSignalStatus.DeliveredAndAcknowledged,
          acknowledgedAt = Some(ackTime)
        )
        copy(
          signals = signals.updated(signalId, updated),
          deliveredCount = deliveredCount + 1,
          lastUpdatedAt = ackTime
        )
      case None => this

/**
 * State maintained by `DocumentVerificationAgent` (Section 6):
 * Coordinates verification of external documents (payslips, tax returns, external debt obligations, KYC)
 * and routes appropriate observed events (`IncomeVerified`, `DebtUpdated`, `ExternalDocumentReceived`).
 */
final case class VerifiedDocumentAuditEntry(
    docId: String,
    customerId: String,
    factType: DocumentFactType,
    derivedEventType: String,
    issuer: String,
    verificationStatus: String,
    verifiedAt: Instant
) extends Serializable

final case class DocumentVerificationAgentState(
    customerId: String,
    verifiedDocuments: Map[String, VerifiedDocumentAuditEntry] = Map.empty,
    totalVerifiedCount: Int = 0,
    lastVerifiedAt: Instant = Instant.EPOCH
) extends Serializable:

  def recordVerifiedDocument(fact: VerifiedDocumentFact, derivedEventType: String): DocumentVerificationAgentState =
    val entry = VerifiedDocumentAuditEntry(
      docId = fact.docId,
      customerId = fact.customerId,
      factType = fact.factType,
      derivedEventType = derivedEventType,
      issuer = fact.issuer,
      verificationStatus = "VERIFIED_AND_ROUTED",
      verifiedAt = fact.verifiedAt
    )
    copy(
      verifiedDocuments = verifiedDocuments.updated(fact.docId, entry),
      totalVerifiedCount = totalVerifiedCount + 1,
      lastVerifiedAt = if fact.verifiedAt.isAfter(lastVerifiedAt) then fact.verifiedAt else lastVerifiedAt
    )

/**
 * State maintained by `PortfolioRiskAgent` (Section 6 & 15):
 * Interprets risk at the segment or portfolio level (`Portfolio Situation`).
 */
final case class CustomerPortfolioEntry(
    customerId: String,
    monthlyIncome: BigDecimal,
    incomeStability: IncomeStability,
    liquidityTrend: LiquidityTrend,
    maxRiskScore: Double,
    completenessScore: Double,
    updatedAt: Instant
) extends Serializable

final case class PortfolioSituationState(
    segmentId: String,
    customers: Map[String, CustomerPortfolioEntry] = Map.empty,
    detectedGlobalPatterns: List[NetworkRiskPatternDetected] = Nil,
    lastUpdatedAt: Instant = Instant.EPOCH
) extends Serializable:

  def updateCustomerEntry(state: CustomerSituationState): PortfolioSituationState =
    val entry = CustomerPortfolioEntry(
      customerId = state.customerId,
      monthlyIncome = state.situationSummary.monthlyIncome,
      incomeStability = state.situationSummary.incomeStability,
      liquidityTrend = state.situationSummary.liquidityTrend,
      maxRiskScore = state.maxNetworkRiskScore,
      completenessScore = state.dataCompleteness.score,
      updatedAt = state.lastUpdatedAt
    )
    copy(
      customers = customers.updated(state.customerId, entry),
      lastUpdatedAt = if state.lastUpdatedAt.isAfter(lastUpdatedAt) then state.lastUpdatedAt else lastUpdatedAt
    )

  def recordGlobalPattern(pattern: NetworkRiskPatternDetected): PortfolioSituationState =
    copy(
      detectedGlobalPatterns = (pattern :: detectedGlobalPatterns).distinctBy(_.patternId).take(50),
      lastUpdatedAt = if pattern.detectedAt.isAfter(lastUpdatedAt) then pattern.detectedAt else lastUpdatedAt
    )

  def activeCustomersCount: Int = customers.size

  def highRiskCustomerCount: Int =
    customers.valuesIterator.count(c => c.maxRiskScore >= 0.45 || c.liquidityTrend == LiquidityTrend.Critical)

  def riskConcentrationRatio: Double =
    if customers.isEmpty then 0.0
    else BigDecimal(highRiskCustomerCount.toDouble / customers.size.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

  def averageMonthlyIncome: BigDecimal =
    if customers.isEmpty then BigDecimal(0)
    else (customers.valuesIterator.map(_.monthlyIncome).sum / BigDecimal(customers.size)).setScale(2, BigDecimal.RoundingMode.HALF_UP)

/**
 * Global Property Graph Vertex and Edge models stored in Oracle Exadata `SA_GRAPH` (Section 5 & 11).
 */
final case class GraphVertex(
    vertexId: String,
    vertexType: String, // "BANK_CUSTOMER", "EXTERNAL_ENTITY", "RESTRICTED_ENTITY"
    segmentId: String,
    intrinsicRisk: Double,
    degreeCentrality: Int,
    communityId: String,
    lastUpdatedAt: Instant
) extends Serializable

final case class GraphEdge(
    edgeId: String,
    sourceVertexId: String,
    targetVertexId: String,
    relationshipType: String,
    totalVolume: BigDecimal,
    txCount: Int,
    evidenceTxIds: List[String],
    lastActiveAt: Instant
) extends Serializable

/**
 * Hierarchical Distributed Situation Awareness Summary (Section 15):
 * Customer Situation -> Relationship Situation -> Portfolio Situation -> Global Banking Situation
 */
final case class GlobalBankingSituation(
    activeCustomersCount: Int,
    activeRelationshipsCount: Int,
    activeRiskSignalsCount: Int,
    verifiedDocumentsCount: Int,
    portfolioSegments: List[PortfolioSituationState],
    globalGraphVerticesCount: Int,
    globalGraphEdgesCount: Int,
    detectedGlobalPatterns: List[NetworkRiskPatternDetected],
    oracleSchemasStatus: Map[String, Int],
    evaluatedAt: Instant
) extends Serializable

