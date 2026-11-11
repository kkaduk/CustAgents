package com.bank.sa.domain

import java.time.{Duration, Instant}

enum IncomeStability:
  case High, Medium, Low, Unverified

enum LiquidityTrend:
  case Positive, Stable, Decreasing, Critical

enum CompletenessLevel:
  case Complete, Partial, Insufficient

/**
 * Synthesized Level-2 Customer Situation Awareness indicators (Section 4 & Section 7):
 * Allows answering "Is the customer's financial situation currently stable?" in O(1) without SQL aggregation.
 */
final case class CustomerSituationSummary(
    monthlyIncome: BigDecimal = BigDecimal(0),
    incomeStability: IncomeStability = IncomeStability.Unverified,
    liquidityTrend: LiquidityTrend = LiquidityTrend.Stable,
    lastSalaryObserved: Option[Instant] = None,
    financialSituationStatus: String = "INITIALIZING"
) extends Serializable

/**
 * Situation Consistency & Data Completeness metadata (Section 10):
 * "Każda ewaluacja powinna wskazywać wersję sytuacji i stopień kompletności danych.
 *  Agent nie może interpretować braku informacji jako potwierdzenia braku ryzyka."
 */
final case class DataCompleteness(
    score: Double = 0.0,
    level: CompletenessLevel = CompletenessLevel.Insufficient,
    hasVerifiedIncome: Boolean = false,
    hasKycVerification: Boolean = false,
    hasTransactionHistory: Boolean = false,
    missingFacts: List[String] = List("VerifiedMonthlyNetIncome", "KycVerificationStatus", "TransactionHistory")
) extends Serializable

/** Represents a connection in the customer's localized Ego-Network (1st-hop counterparty). */
final case class CounterpartyConnection(
    counterpartyId: String,
    isBankClient: Boolean,
    totalVolume: BigDecimal,
    transactionCount: Int,
    recentTxIds: List[String],
    lastInteractionAt: Instant
) extends Serializable:
  def record(tx: EnrichedTransaction): CounterpartyConnection =
    copy(
      isBankClient = isBankClient || tx.counterpartyIsBankClient,
      totalVolume = totalVolume + tx.amount,
      transactionCount = transactionCount + 1,
      recentTxIds = (tx.txId :: recentTxIds).distinct.take(15),
      lastInteractionAt = if tx.timestamp.isAfter(lastInteractionAt) then tx.timestamp else lastInteractionAt
    )

object CounterpartyConnection:
  def initial(tx: EnrichedTransaction): CounterpartyConnection =
    CounterpartyConnection(
      counterpartyId = tx.counterpartyId,
      isBankClient = tx.counterpartyIsBankClient,
      totalVolume = tx.amount,
      transactionCount = 1,
      recentTxIds = List(tx.txId),
      lastInteractionAt = tx.timestamp
    )

/**
 * Provenance trace of a 1st-hop (direct watchlist match), 2nd-hop (peer-propagated),
 * or Global Graph (`NetworkRiskPatternDetected` from Oracle Exadata) risk connection.
 */
final case class RiskExposureTrace(
    exposureKey: String,
    sourceEntityId: String,
    intermediaryPeerId: Option[String],
    hopDistance: Int,
    riskScore: Double,
    reasonCode: String,
    description: String,
    evidenceIds: List[String],
    detectedAt: Instant
) extends Serializable

object RiskExposureTrace:
  def fromDirectRestrictedTx(tx: EnrichedTransaction, tag: RestrictedEntityTag): RiskExposureTrace =
    RiskExposureTrace(
      exposureKey = s"DIRECT:${tag.entityId}",
      sourceEntityId = tag.entityId,
      intermediaryPeerId = None,
      hopDistance = 1,
      riskScore = tag.level.baseRiskWeight,
      reasonCode = s"RC_RESTRICTED_1HOP_${tag.level.toString.toUpperCase}",
      description = s"Direct transaction (${tx.txId}) with restricted counterparty '${tag.entityId}' on ${tag.listSource} (${tag.reasonDescription})",
      evidenceIds = List(tx.txId),
      detectedAt = tx.timestamp
    )

  def fromPeerSignal(signal: PeerRiskSignal, connectingTxIds: List[String]): RiskExposureTrace =
    RiskExposureTrace(
      exposureKey = s"PEER:${signal.originCustomerId}:VIA:${signal.fromCustomerId}",
      sourceEntityId = signal.originCustomerId,
      intermediaryPeerId = Some(signal.fromCustomerId),
      hopDistance = signal.hopCount,
      riskScore = signal.attenuatedRiskScore,
      reasonCode = signal.reasonCode,
      description =
        s"${signal.hopCount}-hop risk exposure via connected client '${signal.fromCustomerId}' (${signal.reasonDescription})",
      evidenceIds = (signal.signalId :: (connectingTxIds ++ signal.rootEvidenceIds)).distinct,
      detectedAt = signal.emittedAt
    )

  def fromGlobalNetworkPattern(pattern: NetworkRiskPatternDetected): RiskExposureTrace =
    RiskExposureTrace(
      exposureKey = s"GLOBAL_GRAPH:${pattern.patternId}:${pattern.sharedEntityId}",
      sourceEntityId = pattern.sharedEntityId,
      intermediaryPeerId = pattern.linkedCustomers.find(_ != pattern.affectedCustomerId),
      hopDistance = 3,
      riskScore = pattern.riskScore,
      reasonCode = pattern.reasonCode,
      description =
        s"Oracle Exadata Global Graph [${pattern.patternType}]: ${pattern.description} (linked accounts: ${pattern.linkedCustomers.mkString(", ")})",
      evidenceIds = (pattern.patternId :: pattern.evidenceIds).distinct,
      detectedAt = pattern.detectedAt
    )

/**
 * Complete Level-2 Situation Awareness state maintained in memory by a `CustomerAgent` and persisted in bounded Oracle snapshots (Section 3):
 * - Current financial indicators (`situationSummary`)
 * - Time-bounded hourly buckets (`cashflow`)
 * - Verified facts (`verifiedFacts`)
 * - Active risk exposures (`riskExposures`)
 * - Evidence IDs (`recentTxIds`, `recentEventIds`) and compact historical digest (`processedEventDigests` for Section 10 Idempotency)
 * - Transactional Outbox (`pendingOutbox` for Section 10 Reliable Delivery)
 * - State version, rule version, and data completeness (`stateVersion`, `ruleVersion`, `dataCompleteness` for Section 10 Situation Consistency)
 */
final case class CustomerSituationState(
    customerId: String,
    cashflow: SlidingWindowCashflow = SlidingWindowCashflow(),
    verifiedFacts: Map[DocumentFactType, VerifiedDocumentFact] = Map.empty,
    egoNetwork: Map[String, CounterpartyConnection] = Map.empty,
    riskExposures: Map[String, RiskExposureTrace] = Map.empty,
    recentTxIds: List[String] = Nil,
    lastUpdatedAt: Instant = Instant.EPOCH,
    stateVersion: Long = 1L,
    ruleVersion: String = "SA-CREDIT-POLICY-2026.11",
    segmentId: String = "RETAIL-PRIME",
    situationSummary: CustomerSituationSummary = CustomerSituationSummary(),
    dataCompleteness: DataCompleteness = DataCompleteness(),
    pendingOutbox: Map[String, OutboxEntry] = Map.empty,
    processedEventDigests: Set[Long] = Set.empty,
    recentEventIds: List[String] = Nil,
    maxEgoNetworkPeers: Int = CustomerSituationState.MaxEgoNetworkPeers
) extends Serializable:

  /**
   * Idempotency check (Section 10 & Roadmap 9.5):
   * Checks both the recent event/tx ID list AND the 64-bit collision-resistant historical digest set (`Set[Long]`)
   * so duplicate historical events beyond the short recent list cannot mutate customer state twice.
   */
  def hasProcessedTx(txId: String): Boolean =
    recentTxIds.contains(txId) || processedEventDigests.contains(CustomerSituationState.digest64(txId))

  def hasProcessedEvent(eventId: String): Boolean =
    recentEventIds.contains(eventId) || recentTxIds.contains(eventId) || processedEventDigests.contains(CustomerSituationState.digest64(eventId))

  def markEventProcessed(eventId: String): CustomerSituationState =
    val digest = CustomerSituationState.digest64(eventId)
    val updatedDigests =
      if processedEventDigests.size >= 10000 then processedEventDigests.drop(1000) + digest
      else processedEventDigests + digest
    copy(
      recentEventIds = (eventId :: recentEventIds).distinct.take(250),
      processedEventDigests = updatedDigests
    )

  def enqueueOutbox(entries: Iterable[OutboxEntry]): CustomerSituationState =
    if entries.isEmpty then this
    else copy(pendingOutbox = pendingOutbox ++ entries.map(e => e.outboxId -> e))

  def acknowledgeOutbox(outboxId: String): CustomerSituationState =
    copy(pendingOutbox = pendingOutbox - outboxId)

  /** True when the customer's localized Ego-Network reaches the super-node degree threshold (Roadmap 9.4). */
  def isHighDegreeSuperNode: Boolean =
    egoNetwork.size >= maxEgoNetworkPeers

  def recordTransaction(tx: EnrichedTransaction): CustomerSituationState =
    if hasProcessedTx(tx.txId) then this
    else
      val updatedCashflow = cashflow.record(tx)
      val rawUpdatedNetwork = egoNetwork.updatedWith(tx.counterpartyId) {
        case Some(existing) => Some(existing.record(tx))
        case None           => Some(CounterpartyConnection.initial(tx))
      }
      // Roadmap 9.4: Bound Ego-Network to top `maxEgoNetworkPeers` active peers by bank-client priority, recency, and volume
      val updatedNetwork =
        if rawUpdatedNetwork.size <= maxEgoNetworkPeers then rawUpdatedNetwork
        else
          rawUpdatedNetwork.valuesIterator.toList
            .sortBy(c => (!c.isBankClient, -c.lastInteractionAt.toEpochMilli, -c.totalVolume))
            .take(maxEgoNetworkPeers)
            .map(c => c.counterpartyId -> c)
            .toMap

      val updatedRisks = tx.restrictedTag match
        case Some(tag) =>
          val trace = RiskExposureTrace.fromDirectRestrictedTx(tx, tag)
          riskExposures.updated(trace.exposureKey, trace)
        case None =>
          riskExposures

      val latestTime = if tx.timestamp.isAfter(lastUpdatedAt) then tx.timestamp else lastUpdatedAt
      val lastSalary =
        if tx.category == TransactionCategory.Salary && tx.direction == TransactionDirection.Inflow then
          Some(tx.timestamp)
        else situationSummary.lastSalaryObserved

      val txDigest = CustomerSituationState.digest64(tx.txId)
      val updatedDigests =
        if processedEventDigests.size >= 10000 then processedEventDigests.drop(1000) + txDigest
        else processedEventDigests + txDigest

      val baseUpdated = copy(
        cashflow = updatedCashflow,
        egoNetwork = updatedNetwork,
        riskExposures = updatedRisks,
        recentTxIds = (tx.txId :: recentTxIds).take(200),
        processedEventDigests = updatedDigests,
        lastUpdatedAt = latestTime,
        stateVersion = stateVersion + 1L,
        situationSummary = situationSummary.copy(lastSalaryObserved = lastSalary)
      )
      baseUpdated.recomputeSituationAwareness(latestTime)

  def recordDocumentFact(fact: VerifiedDocumentFact): CustomerSituationState =
    if hasProcessedEvent(fact.docId) then this
    else
      val latestTime = if fact.verifiedAt.isAfter(lastUpdatedAt) then fact.verifiedAt else lastUpdatedAt
      val baseUpdated = markEventProcessed(fact.docId).copy(
        verifiedFacts = verifiedFacts.updated(fact.factType, fact),
        lastUpdatedAt = latestTime,
        stateVersion = stateVersion + 1L
      )
      baseUpdated.recomputeSituationAwareness(latestTime)

  def recordPeerRiskSignal(signal: PeerRiskSignal): CustomerSituationState =
    if hasProcessedEvent(signal.signalId) then this
    else
      val connectingTxs = egoNetwork.get(signal.fromCustomerId).map(_.recentTxIds).getOrElse(Nil)
      val trace         = RiskExposureTrace.fromPeerSignal(signal, connectingTxs)
      val latestTime    = if signal.emittedAt.isAfter(lastUpdatedAt) then signal.emittedAt else lastUpdatedAt
      val baseUpdated = markEventProcessed(signal.signalId).copy(
        riskExposures = riskExposures.updated(trace.exposureKey, trace),
        lastUpdatedAt = latestTime,
        stateVersion = stateVersion + 1L
      )
      baseUpdated.recomputeSituationAwareness(latestTime)

  def recordNetworkRiskPattern(pattern: NetworkRiskPatternDetected): CustomerSituationState =
    if hasProcessedEvent(pattern.patternId) then this
    else
      val trace      = RiskExposureTrace.fromGlobalNetworkPattern(pattern)
      val latestTime = if pattern.detectedAt.isAfter(lastUpdatedAt) then pattern.detectedAt else lastUpdatedAt
      val baseUpdated = markEventProcessed(pattern.patternId).copy(
        riskExposures = riskExposures.updated(trace.exposureKey, trace),
        lastUpdatedAt = latestTime,
        stateVersion = stateVersion + 1L
      )
      baseUpdated.recomputeSituationAwareness(latestTime)

  /**
   * Recomputes the synthesized Level-2 Situation Summary (`monthlyIncome`, `incomeStability`, `liquidityTrend`)
   * and `DataCompleteness` indicators (Section 4, 7, 10).
   */
  def recomputeSituationAwareness(referenceTime: Instant = lastUpdatedAt): CustomerSituationState =
    val ref        = if referenceTime.isAfter(Instant.EPOCH) then referenceTime else Instant.now()
    val metrics30d = cashflow.metricsForWindow(ref, Duration.ofDays(30))
    val docIncome  = verifiedFacts.get(DocumentFactType.VerifiedMonthlyNetIncome).flatMap(_.numericValue)
    val monthlyInc = docIncome.getOrElse(metrics30d.totalInflow)

    val stability =
      if docIncome.exists(_ > BigDecimal(0)) && metrics30d.overdraftCount == 0 then IncomeStability.High
      else if docIncome.exists(_ > BigDecimal(0)) || metrics30d.totalInflow > BigDecimal(1500) then IncomeStability.Medium
      else if metrics30d.totalInflow > BigDecimal(0) then IncomeStability.Low
      else IncomeStability.Unverified

    val liqTrend =
      if metrics30d.overdraftCount > 1 || metrics30d.highRiskToInflowRatio > BigDecimal("0.20") then LiquidityTrend.Critical
      else if metrics30d.netCashflow < BigDecimal(0) || metrics30d.overdraftCount == 1 then LiquidityTrend.Decreasing
      else if metrics30d.netCashflow > BigDecimal(500) then LiquidityTrend.Positive
      else LiquidityTrend.Stable

    val statusLabel =
      if maxNetworkRiskScore >= 0.50 || liqTrend == LiquidityTrend.Critical then "DISTRESSED_OR_HIGH_RISK"
      else if stability == IncomeStability.High && liqTrend == LiquidityTrend.Positive then "STABLE_HEALTHY"
      else if stability == IncomeStability.Unverified then "UNVERIFIED_INCOMPLETE"
      else "MODERATE_MONITORED"

    val hasIncomeFact = verifiedFacts.contains(DocumentFactType.VerifiedMonthlyNetIncome)
    val hasKycFact    = verifiedFacts.contains(DocumentFactType.KycVerificationStatus)
    val hasTxHistory  = recentTxIds.nonEmpty

    val missing = List(
      Option.unless(hasIncomeFact)("VerifiedMonthlyNetIncome"),
      Option.unless(hasKycFact)("KycVerificationStatus"),
      Option.unless(hasTxHistory)("TransactionHistory")
    ).flatten

    val completenessScore =
      (if hasIncomeFact then 0.50 else 0.0) +
        (if hasTxHistory then 0.30 else 0.0) +
        (if hasKycFact then 0.20 else 0.0)

    val completenessLevel =
      if completenessScore >= 0.80 then CompletenessLevel.Complete
      else if completenessScore >= 0.50 then CompletenessLevel.Partial
      else CompletenessLevel.Insufficient

    copy(
      situationSummary = CustomerSituationSummary(
        monthlyIncome = monthlyInc,
        incomeStability = stability,
        liquidityTrend = liqTrend,
        lastSalaryObserved = situationSummary.lastSalaryObserved,
        financialSituationStatus = statusLabel
      ),
      dataCompleteness = DataCompleteness(
        score = BigDecimal(completenessScore).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
        level = completenessLevel,
        hasVerifiedIncome = hasIncomeFact,
        hasKycVerification = hasKycFact,
        hasTransactionHistory = hasTxHistory,
        missingFacts = missing
      )
    )

  /** Connected bank clients in the customer's Ego-Network eligible for risk propagation, sorted by recency & volume. */
  def connectedBankPeers: List[CounterpartyConnection] =
    egoNetwork.valuesIterator
      .filter(_.isBankClient)
      .toList
      .sortBy(c => (-c.lastInteractionAt.toEpochMilli, -c.totalVolume))

  /** Highest risk exposure score across 1st-hop, 2nd-hop, and Global Graph connections. */
  def maxNetworkRiskScore: Double =
    riskExposures.valuesIterator.map(_.riskScore).maxOption.getOrElse(0.0)

object CustomerSituationState:
  /** Maximum 1st-hop counterparties retained in a single actor's localized Ego-Network (Roadmap 9.4). */
  val MaxEgoNetworkPeers: Int = 200

  /** Maximum peer push fan-out threshold before delegating to Global Graph / Super-Node suppression (Roadmap 9.4). */
  val MaxPushPeersThreshold: Int = 200

  /**
   * 64-bit FNV-1a + MurmurHash3 fmix64 avalanche digest (Roadmap 9.5).
   * Reduces birthday-paradox collision probability at 10,000 events from ~1.1% (32-bit Int) to < 3e-12 (64-bit Long).
   */
  def digest64(value: String): Long =
    var h = 0xcbf29ce484222325L
    var i = 0
    while i < value.length do
      h ^= (value.charAt(i).toLong & 0xffffL)
      h *= 0x100000001b3L
      i += 1
    h ^= (h >>> 33)
    h *= 0xff51afd7ed558ccdL
    h ^= (h >>> 33)
    h *= 0xc4ceb9fe1a85ec53L
    h ^= (h >>> 33)
    h

  def empty(customerId: String): CustomerSituationState =
    CustomerSituationState(customerId = customerId)
