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
