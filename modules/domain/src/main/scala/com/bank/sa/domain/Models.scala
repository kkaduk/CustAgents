package com.bank.sa.domain

import java.time.Instant

enum TransactionDirection:
  case Inflow, Outflow

enum TransactionCategory:
  case Salary
  case EverydaySpend
  case Housing
  case Utilities
  case LoanRepayment
  case Gambling
  case CryptoExchange
  case PeerTransfer
  case HighRiskTransfer

  def isHighRiskCategory: Boolean = this match
    case Gambling | HighRiskTransfer => true
    case _                           => false

enum RestrictionLevel(val baseRiskWeight: Double):
  case SanctionedEntity  extends RestrictionLevel(1.00)
  case FraudWatchlist    extends RestrictionLevel(0.90)
  case UnlicensedLender  extends RestrictionLevel(0.75)
  case HighRiskWatchlist extends RestrictionLevel(0.60)

/** Metadata attached by the Flink Broadcast State when a counterparty matches a restricted list. */
final case class RestrictedEntityTag(
    entityId: String,
    level: RestrictionLevel,
    listSource: String,
    reasonDescription: String
) extends Serializable

/** Raw transaction event arriving on Kafka from core payment rails. */
final case class RawTransaction(
    txId: String,
    fromCustomerId: String,
    toCounterpartyId: String,
    counterpartyIsBankClient: Boolean,
    amount: BigDecimal,
    currency: String,
    category: TransactionCategory,
    resultsInOverdraft: Boolean,
    timestamp: Instant,
    merchantDescription: Option[String] = None
) extends Serializable

/**
 * Enriched transaction emitted by the Flink pipeline (including external categorizer module)
 * and keyed by `customerId` so it lands directly on the corresponding Pekko Cluster Shard.
 */
final case class EnrichedTransaction(
    txId: String,
    customerId: String,
    counterpartyId: String,
    counterpartyIsBankClient: Boolean,
    direction: TransactionDirection,
    amount: BigDecimal,
    currency: String,
    category: TransactionCategory,
    resultsInOverdraft: Boolean,
    restrictedTag: Option[RestrictedEntityTag],
    timestamp: Instant,
    categorizerSource: String = "ExternalCategorizerModule"
) extends Serializable

enum DocumentFactType:
  case VerifiedMonthlyNetIncome
  case EmploymentContractStatus
  case ExternalDebtObligation
  case TaxReturnAnnualIncome
  case KycVerificationStatus

/** Structured fact extracted from an external document (e.g., payslip, tax return, credit bureau). */
final case class VerifiedDocumentFact(
    docId: String,
    customerId: String,
    factType: DocumentFactType,
    numericValue: Option[BigDecimal],
    textValue: Option[String],
    issuer: String,
    verifiedAt: Instant
) extends Serializable

/**
 * Asynchronous risk signal propagated between agents in the Pekko cluster
 * with Reliable Delivery & Transactional Outbox deduplication guarantees (Section 10).
 */
final case class PeerRiskSignal(
    signalId: String,
    originCustomerId: String,
    fromCustomerId: String,
    targetCustomerId: String,
    hopCount: Int,
    attenuatedRiskScore: Double,
    reasonCode: String,
    reasonDescription: String,
    rootEvidenceIds: List[String],
    emittedAt: Instant,
    outboxId: Option[String] = None
) extends Serializable

/**
 * Global Graph Risk Intelligence pattern types detected by Oracle Exadata Property Graph (`SA_GRAPH` / `SA_INTELLIGENCE`, Section 5 & 14).
 */
enum NetworkRiskPatternType:
  case SharedHighRiskCounterparty
  case SimilarTransactionPattern
  case SegmentRiskConcentration
  case HighDegreeNodeExposure
  case OrganizedRiskCommunity

/**
 * Risk intelligence event emitted by Oracle Exadata Global Relationship Graph analysis (`NetworkRiskPatternDetected`, Section 5 & 14)
 * and delivered via Kafka to Pekko CustomerAgents and PortfolioRiskAgents.
 */
final case class NetworkRiskPatternDetected(
    patternId: String,
    patternType: NetworkRiskPatternType,
    affectedCustomerId: String,
    sharedEntityId: String,
    linkedCustomers: List[String],
    riskScore: Double,
    reasonCode: String,
    description: String,
    evidenceIds: List[String],
    detectedAt: Instant
) extends Serializable

/**
 * Observed domain events processed by `CustomerAgent` via the configurable Plugin Architecture (Section 2.1):
 * - Event 1001: TransactionObserved
 * - Event 1002: IncomeVerified
 * - Event 1003: DebtUpdated
 * - Event 1004: CounterpartyRiskReceived
 * - Event 1005: ExternalDocumentReceived
 * - Event 1006: GlobalRiskIntelligenceReceived (NetworkRiskPatternDetected)
 */
sealed trait AgentObservedEvent extends Serializable:
  def eventId: String
  def customerId: String
  def occurredAt: Instant
  def eventType: String

final case class TransactionObserved(
    eventId: String,
    customerId: String,
    tx: EnrichedTransaction,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "TransactionObserved"

final case class IncomeVerified(
    eventId: String,
    customerId: String,
    fact: VerifiedDocumentFact,
    monthlyNetIncome: BigDecimal,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "IncomeVerified"

final case class DebtUpdated(
    eventId: String,
    customerId: String,
    fact: VerifiedDocumentFact,
    monthlyDebtAmount: BigDecimal,
    creditor: String,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "DebtUpdated"

final case class CounterpartyRiskReceived(
    eventId: String,
    customerId: String,
    signal: PeerRiskSignal,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "CounterpartyRiskReceived"

final case class ExternalDocumentReceived(
    eventId: String,
    customerId: String,
    fact: VerifiedDocumentFact,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "ExternalDocumentReceived"

final case class GlobalRiskIntelligenceReceived(
    eventId: String,
    customerId: String,
    pattern: NetworkRiskPatternDetected,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "NetworkRiskPatternDetected"

/**
 * Transactional Outbox entry persisted atomically with the agent's journal event
 * to guarantee reliable side-effect delivery even if the node crashes before sending (Section 10).
 */
enum OutboxTargetType:
  case CustomerAgent, RelationshipAgent, RiskSignalAgent, PortfolioRiskAgent, OracleProjection

final case class OutboxEntry(
    outboxId: String,
    sourceCustomerId: String,
    targetType: OutboxTargetType,
    targetEntityId: String,
    messageType: String,
    payloadSummary: String,
    peerSignal: Option[PeerRiskSignal] = None,
    createdAt: Instant
) extends Serializable

/** Customer loan request arriving from the Loan Origination Gateway. */
final case class LoanApplication(
    applicationId: String,
    customerId: String,
    requestedAmount: BigDecimal,
    termMonths: Int,
    annualInterestRate: BigDecimal,
    requestedAt: Instant
) extends Serializable:
  /** Computes standard monthly installment using amortized loan formula. */
  def estimatedMonthlyInstallment: BigDecimal =
    if termMonths <= 0 then requestedAmount
    else if annualInterestRate <= BigDecimal(0) then
      (requestedAmount / BigDecimal(termMonths)).setScale(2, BigDecimal.RoundingMode.HALF_UP)
    else
      val monthlyRate = annualInterestRate.toDouble / 12.0
      val factor      = math.pow(1.0 + monthlyRate, termMonths.toDouble)
      val payment     = requestedAmount.toDouble * (monthlyRate * factor) / (factor - 1.0)
      BigDecimal(payment).setScale(2, BigDecimal.RoundingMode.HALF_UP)
