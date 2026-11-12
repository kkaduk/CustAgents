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
  case InternalOwnAccountTransfer

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
    merchantDescription: Option[String] = None,
    fromAccountId: Option[String] = None,
    toAccountId: Option[String] = None
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
    categorizerSource: String = "ExternalCategorizerModule",
    fromAccountId: Option[String] = None,
    toAccountId: Option[String] = None,
    isOwnAccountTransfer: Boolean = false
) extends Serializable

// =============================================================================
// NEW BUSINESS NEEDS MODELS (Doc/NowePotrzeby.md Sections 2, 3, 4, 5, 6)
// =============================================================================

/** Bank account owned by a customer (`CUSTOMER_ACCOUNT` / `BANK_ACCOUNT` in `SA_GRAPH`, Section 2 & 3.3). */
final case class CustomerAccount(
    accountId: String,
    customerId: String,
    accountType: String = "CHECKING",
    currency: String = "PLN",
    openedAt: Instant = Instant.EPOCH
) extends Serializable

/** Bank employee profile (`EMPLOYEE` vertex in `SA_GRAPH`, Section 2). */
final case class EmployeeProfile(
    employeeId: String,
    fullName: String,
    role: String,
    branchId: String,
    phoneNumberRaw: String,
    phoneNumberNormalized: String,
    servicedCustomerIds: List[String],
    registeredAt: Instant
) extends Serializable

/** Raw customer contact/phone change event arriving on Kafka (Section 2). */
final case class RawCustomerContactChange(
    eventId: String,
    customerId: String,
    previousPhoneRaw: Option[String],
    newPhoneRaw: String,
    sourceSystem: String = "CRM-CONTACT-REGISTRY",
    changedByEmployeeId: Option[String] = None,
    occurredAt: Instant = Instant.now()
) extends Serializable

/** Flink-normalized customer contact change with employee phone match resolution (Section 2). */
final case class NormalizedCustomerContactChange(
    eventId: String,
    customerId: String,
    previousPhoneNormalized: Option[String],
    newPhoneNormalized: String,
    sourceSystem: String,
    changedByEmployeeId: Option[String],
    matchedEmployeeId: Option[String] = None,
    matchedEmployeeName: Option[String] = None,
    employeeServicesCustomer: Boolean = false,
    occurredAt: Instant
) extends Serializable

/** First-seen account record (`AccountFirstSeen` in Section 4.1). */
final case class AccountFirstSeen(
    accountId: String,
    firstObservedTxId: String,
    firstObservedCustomerId: String,
    firstSeenAt: Instant
) extends Serializable

/** Account history & 1 PLN verification result (`SA_FACTS` & `SA_GRAPH`, Section 4). */
final case class AccountVerificationResult(
    accountId: String,
    knownInBank: Boolean,
    ownerCustomerId: Option[String],
    firstSeenAt: Option[Instant],
    lastSeenAt: Option[Instant],
    transactionCount: Int,
    totalVolume: BigDecimal,
    linkedCustomers: List[String],
    highRiskConnections: List[String],
    suspiciousTxIds: List[String],
    assessment: String,
    riskSignalCode: Option[String],
    checkedAt: Instant
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
 * Global Graph Risk Intelligence pattern types detected by Oracle Exadata Property Graph
 * (`SA_GRAPH` / `SA_INTELLIGENCE`, Doc/ExaPekdoDesign.md Section 5 & 14 + Doc/NowePotrzeby.md Section 2 & 3.2).
 */
enum NetworkRiskPatternType:
  case SharedHighRiskCounterparty
  case SimilarTransactionPattern
  case SegmentRiskConcentration
  case HighDegreeNodeExposure
  case OrganizedRiskCommunity
  case RapidPassThrough
  case FanInFanOut
  case MultiHopMoneyMovement
  case CircularTransfers
  case EmployeeCustomerPhoneMatch

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
 * Event 1007 (Doc/NowePotrzeby.md Section 2):
 * Customer phone/contact changed — routed to `CustomerContactChangedPlugin` via `EventPluginRegistry`.
 */
final case class CustomerContactChanged(
    eventId: String,
    customerId: String,
    change: NormalizedCustomerContactChange,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "CustomerContactChanged"

/**
 * Event 1008 (Doc/NowePotrzeby.md Section 4.2):
 * Account history verification risk signal (e.g. 1 PLN verification transfer to unknown or high-risk account)
 * routed to `AccountHistoryRiskPlugin` via `EventPluginRegistry`.
 */
final case class AccountHistoryRiskDetected(
    eventId: String,
    customerId: String,
    targetAccountId: String,
    verification: AccountVerificationResult,
    transferAmount: BigDecimal,
    occurredAt: Instant
) extends AgentObservedEvent:
  override val eventType: String = "AccountHistoryRiskDetected"

/**
 * Pre-execution payment transfer order evaluated by `TransactionRiskDecisionEngine` BEFORE payment execution
 * (Doc/NowePotrzeby.md Section 6).
 */
final case class ProposedPaymentTransfer(
    transferId: String,
    customerId: String,
    fromAccountId: Option[String],
    toAccountId: String,
    toCounterpartyId: String,
    amount: BigDecimal,
    currency: String = "PLN",
    title: String = "",
    isVerificationOnePlnTransfer: Boolean = false,
    requestedAt: Instant = Instant.now()
) extends Serializable

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
