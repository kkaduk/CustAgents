package com.bank.sa.domain

import java.time.Instant

/**
 * Level-3 Situation Awareness Projection (Section 7):
 * Evaluates potential future consequences of a proposed loan (`Future DTI`, `Expected Liquidity`,
 * `Risk Exposure`, `Affordability`) independently of formal credit decision approval.
 */
final case class SituationProjection(
    applicationId: String,
    customerId: String,
    proposedInstallment: BigDecimal,
    futureDti: BigDecimal,
    expectedMonthlyLiquidity: BigDecimal,
    projectedLiquidityTrend: LiquidityTrend,
    riskExposureScore: Double,
    affordabilityBuffer: BigDecimal,
    isAffordable: Boolean,
    stateVersion: Long,
    ruleVersion: String,
    dataCompleteness: DataCompleteness,
    projectedAt: Instant
) extends Serializable

/**
 * Regulatory Adverse Action Reason Code (compliant with EU AI Act / GDPR Art. 22 / US ECOA).
 * Ties a specific rejection or score penalty to factual live metrics and immutable evidence IDs.
 */
final case class ReasonCode(
    code: String,
    title: String,
    regulatoryStatement: String,
    metricName: String,
    actualValue: String,
    thresholdValue: String,
    evidenceIds: List[String],
    counterfactualGuidance: String,
    isHardKnockout: Boolean,
    scorePenalty: Int
) extends Serializable

/** Trace of a single policy rule or scorecard factor evaluated against the CustomerSituationState. */
final case class RuleEvaluationTrace(
    ruleId: String,
    ruleName: String,
    passed: Boolean,
    pointsDelta: Int,
    reasonCode: Option[ReasonCode]
) extends Serializable

/** Full explainable audit trail persisted alongside every loan decision (Section 7, 10, 13). */
final case class ExplainableAuditTrail(
    policyVersion: String,
    evaluatedAt: Instant,
    baseScore: Int,
    finalCreditScore: Int,
    minimumRequiredScore: Int,
    ruleTraces: List[RuleEvaluationTrace],
    principalAdverseReasons: List[ReasonCode],
    statementOfReasons: String,
    stateVersion: Long = 1L,
    dataCompleteness: DataCompleteness = DataCompleteness(),
    situationProjection: Option[SituationProjection] = None
) extends Serializable

enum DecisionOutcome extends Serializable:
  case Approved(
      approvedAmount: BigDecimal,
      termMonths: Int,
      offeredAnnualRate: BigDecimal,
      monthlyInstallment: BigDecimal,
      creditScore: Int
  )
  case Denied(
      principalReasons: List[ReasonCode],
      creditScore: Int,
      statementOfReasons: String
  )

final case class LoanDecisionResponse(
    applicationId: String,
    customerId: String,
    outcome: DecisionOutcome,
    auditTrail: ExplainableAuditTrail,
    evaluatedAt: Instant
) extends Serializable

// =============================================================================
// PRE-EXECUTION TRANSACTION RISK DECISION ENGINE MODELS (Doc/NowePotrzeby.md Section 6)
// =============================================================================

/**
 * Operational payment authorization decision returned to the payment system before transfer execution:
 * - `ALLOW`       — Transfer proceeds immediately
 * - `REVIEW_HOLD` — Transfer is placed on hold for analyst/customer verification
 * - `BLOCK`       — Transfer is blocked prior to funds leaving the bank
 */
enum TransactionAuthorizationDecision:
  case ALLOW, REVIEW_HOLD, BLOCK

/**
 * Full explainable response produced by `TransactionRiskDecisionEngine` when evaluating a `ProposedPaymentTransfer`
 * against the customer's pre-computed `CustomerSituationState` and `AccountVerificationResult` (Doc/NowePotrzeby.md Section 6).
 */
final case class TransactionAuthorizationResponse(
    transferId: String,
    customerId: String,
    fromAccountId: Option[String],
    toAccountId: String,
    toCounterpartyId: String,
    amount: BigDecimal,
    currency: String,
    decision: TransactionAuthorizationDecision,
    riskScore: Int,
    reviewThresholdScore: Int,
    blockThresholdScore: Int,
    policyVersion: String,
    ruleTraces: List[RuleEvaluationTrace],
    principalReasons: List[ReasonCode],
    statementOfReasons: String,
    accountVerification: AccountVerificationResult,
    stateVersion: Long,
    stateFreshnessMs: Long,
    responseTimeMs: Long,
    fallbackModeUsed: Boolean,
    paymentRailDirective: String,
    evaluatedAt: Instant
) extends Serializable
