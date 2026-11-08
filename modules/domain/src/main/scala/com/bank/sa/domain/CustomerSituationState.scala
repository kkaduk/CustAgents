package com.bank.sa.domain

import java.time.Instant

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
 * Provenance trace of a 1st-hop (direct watchlist match) or 2nd-hop (peer-propagated)
 * risk connection. Provides exact evidence IDs required by regulatory explainability.
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

/**
 * Complete Level-2 Situation Awareness state maintained in memory by a `CustomerAgent`.
 */
final case class CustomerSituationState(
    customerId: String,
    cashflow: SlidingWindowCashflow = SlidingWindowCashflow(),
    verifiedFacts: Map[DocumentFactType, VerifiedDocumentFact] = Map.empty,
    egoNetwork: Map[String, CounterpartyConnection] = Map.empty,
    riskExposures: Map[String, RiskExposureTrace] = Map.empty,
    recentTxIds: List[String] = Nil,
    lastUpdatedAt: Instant = Instant.EPOCH
) extends Serializable:

  def hasProcessedTx(txId: String): Boolean = recentTxIds.contains(txId)

  def recordTransaction(tx: EnrichedTransaction): CustomerSituationState =
    if hasProcessedTx(tx.txId) then this
    else
      val updatedCashflow = cashflow.record(tx)
      val updatedNetwork = egoNetwork.updatedWith(tx.counterpartyId) {
        case Some(existing) => Some(existing.record(tx))
        case None           => Some(CounterpartyConnection.initial(tx))
      }
      val updatedRisks = tx.restrictedTag match
        case Some(tag) =>
          val trace = RiskExposureTrace.fromDirectRestrictedTx(tx, tag)
          riskExposures.updated(trace.exposureKey, trace)
        case None =>
          riskExposures

      val latestTime = if tx.timestamp.isAfter(lastUpdatedAt) then tx.timestamp else lastUpdatedAt
      copy(
        cashflow = updatedCashflow,
        egoNetwork = updatedNetwork,
        riskExposures = updatedRisks,
        recentTxIds = (tx.txId :: recentTxIds).take(200),
        lastUpdatedAt = latestTime
      )

  def recordDocumentFact(fact: VerifiedDocumentFact): CustomerSituationState =
    val latestTime = if fact.verifiedAt.isAfter(lastUpdatedAt) then fact.verifiedAt else lastUpdatedAt
    copy(
      verifiedFacts = verifiedFacts.updated(fact.factType, fact),
      lastUpdatedAt = latestTime
    )

  def recordPeerRiskSignal(signal: PeerRiskSignal): CustomerSituationState =
    val connectingTxs = egoNetwork.get(signal.fromCustomerId).map(_.recentTxIds).getOrElse(Nil)
    val trace         = RiskExposureTrace.fromPeerSignal(signal, connectingTxs)
    val latestTime    = if signal.emittedAt.isAfter(lastUpdatedAt) then signal.emittedAt else lastUpdatedAt
    copy(
      riskExposures = riskExposures.updated(trace.exposureKey, trace),
      lastUpdatedAt = latestTime
    )

  /** Connected bank clients in the customer's Ego-Network eligible for risk propagation. */
  def connectedBankPeers: List[CounterpartyConnection] =
    egoNetwork.valuesIterator.filter(_.isBankClient).toList

  /** Highest risk exposure score across 1st-hop and 2nd-hop connections. */
  def maxNetworkRiskScore: Double =
    riskExposures.valuesIterator.map(_.riskScore).maxOption.getOrElse(0.0)

object CustomerSituationState:
  def empty(customerId: String): CustomerSituationState =
    CustomerSituationState(customerId = customerId)

