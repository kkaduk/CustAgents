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
    timestamp: Instant
) extends Serializable

/**
 * Enriched transaction emitted by the Flink pipeline and keyed by `customerId`
 * so it lands directly on the corresponding Pekko Cluster Shard.
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
    timestamp: Instant
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
 * Asynchronous risk signal propagated between CustomerAgents in the Pekko cluster
 * to replace expensive centralized graph traversals at decision time.
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
    emittedAt: Instant
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

