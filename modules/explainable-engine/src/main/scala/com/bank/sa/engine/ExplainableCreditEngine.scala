package com.bank.sa.engine

import com.bank.sa.domain.*
import java.time.Duration

/**
 * Policy thresholds for the hybrid deterministic rule + scorecard engine (stored in Oracle `SA_GOVERNANCE`).
 */
final case class CreditPolicyConfig(
    policyVersion: String = "SA-CREDIT-POLICY-2026.11",
    baseScore: Int = 750,
    minimumApprovalScore: Int = 620,
    maxDebtToIncomeRatio: BigDecimal = BigDecimal("0.40"),
    max30DayHighRiskSpendRatio: BigDecimal = BigDecimal("0.15"),
    max30DayOverdraftCount: Int = 1,
    max1HopRiskScore: Double = 0.50,
    max2HopRiskScore: Double = 0.45,
    maxGlobalGraphRiskScore: Double = 0.50
)

/**
 * Pure, deterministic Explainable Credit Decisioning & Level-3 Situation Projection Engine (Section 7, 10, 13).
 *
 * Distinguishes:
 * 1. `projectSituation(state, application)`: Computes future scenario metrics (`Future DTI`, `Expected Liquidity`,
 *    `Risk Exposure`, `Affordability`, `stateVersion`, `dataCompleteness`) independently of formal approval authority.
 * 2. `evaluate(state, application)`: Evaluates a customer's real-time `CustomerSituationState` against a `LoanApplication`
 *    and produces a legally compliant Statement of Reasons with exact evidence IDs, counterfactuals, state version, and completeness.
 */
class ExplainableCreditEngine(val config: CreditPolicyConfig = CreditPolicyConfig()) extends Serializable:

  /**
   * Level-3 Situation Awareness Projection (Section 7):
   * Calculates what will happen to the customer's financial situation if the proposed loan is granted.
   */
  def projectSituation(state: CustomerSituationState, application: LoanApplication): SituationProjection =
    val refTime       = application.requestedAt
    val metrics30d    = state.cashflow.metricsForWindow(refTime, Duration.ofDays(30))
    val installment   = application.estimatedMonthlyInstallment
    val docIncomeOpt  = state.verifiedFacts.get(DocumentFactType.VerifiedMonthlyNetIncome).flatMap(_.numericValue)
    val externalDebt  = state.verifiedFacts.get(DocumentFactType.ExternalDebtObligation).flatMap(_.numericValue).getOrElse(BigDecimal(0))
    val monthlyIncome = docIncomeOpt.getOrElse(metrics30d.totalInflow)

    val totalProjectedDebt = externalDebt + installment
    val futureDti =
      if monthlyIncome > BigDecimal(0) then
        (totalProjectedDebt / monthlyIncome).setScale(4, BigDecimal.RoundingMode.HALF_UP)
      else BigDecimal(1.0)

    val maxAllowedDebt      = (monthlyIncome * config.maxDebtToIncomeRatio).setScale(2, BigDecimal.RoundingMode.HALF_UP)
    val affordabilityBuffer = (maxAllowedDebt - totalProjectedDebt).setScale(2, BigDecimal.RoundingMode.HALF_UP)
    val expectedLiquidity   = (monthlyIncome - metrics30d.totalOutflow - installment).setScale(2, BigDecimal.RoundingMode.HALF_UP)

    val projectedTrend =
      if expectedLiquidity < BigDecimal(0) || metrics30d.overdraftCount > config.max30DayOverdraftCount then
        LiquidityTrend.Critical
      else if expectedLiquidity < installment then
        LiquidityTrend.Decreasing
      else if expectedLiquidity >= installment * 2 then
        LiquidityTrend.Positive
      else
        LiquidityTrend.Stable

    val isAffordable =
      monthlyIncome > BigDecimal(0) &&
        futureDti <= config.maxDebtToIncomeRatio &&
        expectedLiquidity >= BigDecimal(0) &&
        state.maxNetworkRiskScore < config.max2HopRiskScore

    SituationProjection(
      applicationId = application.applicationId,
      customerId = state.customerId,
      proposedInstallment = installment,
      futureDti = futureDti,
      expectedMonthlyLiquidity = expectedLiquidity,
      projectedLiquidityTrend = projectedTrend,
      riskExposureScore = state.maxNetworkRiskScore,
      affordabilityBuffer = affordabilityBuffer,
      isAffordable = isAffordable,
      stateVersion = state.stateVersion,
      ruleVersion = config.policyVersion,
      dataCompleteness = state.dataCompleteness,
      projectedAt = refTime
    )

  /**
   * Formal Explainable Credit Evaluation (Section 7, 10, 13).
   */
  def evaluate(state: CustomerSituationState, application: LoanApplication): LoanDecisionResponse =
    val refTime     = application.requestedAt
    val projection  = projectSituation(state, application)
    val metrics30d  = state.cashflow.metricsForWindow(refTime, Duration.ofDays(30))
    val metrics7d   = state.cashflow.metricsForWindow(refTime, Duration.ofDays(7))
    val installment = application.estimatedMonthlyInstallment

    val baseTraces = List(
      evaluateVerifiedIncomeAndAffordability(state, metrics30d, application, installment),
      evaluateShortTermLiquidityShock(metrics7d, metrics30d),
      evaluateHighRiskSpendVelocity(metrics30d),
      evaluateDirectRestrictedExposure(state),
      evaluateSecondHopPeerContagion(state)
    )

    val globalGraphTraceOpt = evaluateGlobalGraphRiskPattern(state)
    val traces              = baseTraces ++ globalGraphTraceOpt.toList

    val rawScore      = config.baseScore + traces.map(_.pointsDelta).sum
    val finalScore    = math.max(300, math.min(850, rawScore))
    val failedReasons = traces.flatMap(_.reasonCode).sortBy(r => (!r.isHardKnockout, -r.scorePenalty))
    val hasHardStop   = failedReasons.exists(_.isHardKnockout)
    val isApproved    = !hasHardStop && finalScore >= config.minimumApprovalScore && failedReasons.isEmpty

    val statement = buildRegulatoryStatement(
      customerId = state.customerId,
      state = state,
      projection = projection,
      application = application,
      approved = isApproved,
      finalScore = finalScore,
      principalReasons = failedReasons
    )

    val auditTrail = ExplainableAuditTrail(
      policyVersion = config.policyVersion,
      evaluatedAt = refTime,
      baseScore = config.baseScore,
      finalCreditScore = finalScore,
      minimumRequiredScore = config.minimumApprovalScore,
      ruleTraces = traces,
      principalAdverseReasons = failedReasons,
      statementOfReasons = statement,
      stateVersion = state.stateVersion,
      dataCompleteness = state.dataCompleteness,
      situationProjection = Some(projection)
    )

    val outcome =
      if isApproved then
        DecisionOutcome.Approved(
          approvedAmount = application.requestedAmount,
          termMonths = application.termMonths,
          offeredAnnualRate = application.annualInterestRate,
          monthlyInstallment = installment,
          creditScore = finalScore
        )
      else
        DecisionOutcome.Denied(
          principalReasons = failedReasons,
          creditScore = finalScore,
          statementOfReasons = statement
        )

    LoanDecisionResponse(
      applicationId = application.applicationId,
      customerId = state.customerId,
      outcome = outcome,
      auditTrail = auditTrail,
      evaluatedAt = refTime
    )

  private def fmt2(bd: BigDecimal): String =
    bd.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString

  private def fmt1(bd: BigDecimal): String =
    bd.setScale(1, BigDecimal.RoundingMode.HALF_UP).toString

  /** Rule 1: Verified Income & Real-Time Debt-to-Income (DTI) Affordability + Situation Completeness */
  private def evaluateVerifiedIncomeAndAffordability(
      state: CustomerSituationState,
      metrics30d: WindowMetrics,
      app: LoanApplication,
      proposedInstallment: BigDecimal
  ): RuleEvaluationTrace =
    val docIncomeOpt    = state.verifiedFacts.get(DocumentFactType.VerifiedMonthlyNetIncome)
    val externalDebtOpt = state.verifiedFacts.get(DocumentFactType.ExternalDebtObligation)

    val verifiedIncome = docIncomeOpt.flatMap(_.numericValue).getOrElse(metrics30d.totalInflow)
    val externalDebt   = externalDebtOpt.flatMap(_.numericValue).getOrElse(BigDecimal(0))
    val evidenceIds    = (docIncomeOpt.map(_.docId).toList ++ externalDebtOpt.map(_.docId).toList ++ metrics30d.inflowTxIds.take(5)).distinct

    if verifiedIncome <= BigDecimal(0) then
      val reason = ReasonCode(
        code = "RC_101_INSUFFICIENT_VERIFIED_INCOME",
        title = "Unverified or Insufficient Monthly Income (Incomplete Situation Data)",
        regulatoryStatement =
          "Unable to verify sufficient recurring monthly net income from submitted documents or 30-day account inflows. Absence of verified information cannot be interpreted as absence of risk.",
        metricName = "VerifiedMonthlyIncome",
        actualValue = s"${fmt2(verifiedIncome)} EUR (Completeness: ${state.dataCompleteness.level})",
        thresholdValue = s"> ${fmt2(proposedInstallment / config.maxDebtToIncomeRatio)} EUR",
        evidenceIds = evidenceIds,
        counterfactualGuidance =
          s"Provide a verified payslip or tax statement confirming monthly net income of at least ${fmt2(proposedInstallment / config.maxDebtToIncomeRatio)} EUR.",
        isHardKnockout = true,
        scorePenalty = 200
      )
      RuleEvaluationTrace("RULE_AFFORDABILITY_DTI", "Debt-to-Income & Verified Affordability", passed = false, -200, Some(reason))
    else
      val totalMonthlyDebtService = externalDebt + proposedInstallment
      val actualDti = (totalMonthlyDebtService / verifiedIncome).setScale(4, BigDecimal.RoundingMode.HALF_UP)

      if actualDti > config.maxDebtToIncomeRatio then
        val maxAllowedInstallment = fmt2((verifiedIncome * config.maxDebtToIncomeRatio - externalDebt).max(BigDecimal(0)))
        val requiredIncome        = fmt2(totalMonthlyDebtService / config.maxDebtToIncomeRatio)
        val reason = ReasonCode(
          code = "RC_102_EXCESSIVE_DEBT_TO_INCOME",
          title = "Excessive Debt-to-Income Ratio",
          regulatoryStatement =
            s"Projected monthly debt obligations (${fmt2(totalMonthlyDebtService)} EUR including proposed installment of ${fmt2(proposedInstallment)} EUR) represent ${fmt1(actualDti * 100)}% of verified monthly income (${fmt2(verifiedIncome)} EUR), exceeding the maximum policy limit of ${fmt1(config.maxDebtToIncomeRatio * 100)}%.",
          metricName = "DebtToIncomeRatio",
          actualValue = s"${fmt1(actualDti * 100)}%",
          thresholdValue = s"<= ${fmt1(config.maxDebtToIncomeRatio * 100)}%",
          evidenceIds = evidenceIds,
          counterfactualGuidance =
            s"Reduce the requested loan installment to at most $maxAllowedInstallment EUR/month (or increase verified net monthly income to $requiredIncome EUR).",
          isHardKnockout = false,
          scorePenalty = 140
        )
        RuleEvaluationTrace("RULE_AFFORDABILITY_DTI", "Debt-to-Income & Verified Affordability", passed = false, -140, Some(reason))
      else
        RuleEvaluationTrace("RULE_AFFORDABILITY_DTI", "Debt-to-Income & Verified Affordability", passed = true, +25, None)

  /** Rule 2: Short-Term Liquidity Shock & Overdraft Frequency (7d & 30d Windows) */
  private def evaluateShortTermLiquidityShock(
      metrics7d: WindowMetrics,
      metrics30d: WindowMetrics
  ): RuleEvaluationTrace =
    if metrics30d.overdraftCount > config.max30DayOverdraftCount then
      val reason = ReasonCode(
        code = "RC_201_RECENT_OVERDRAFT_EVENTS",
        title = "Recent Account Overdraft Activity",
        regulatoryStatement =
          s"Account recorded ${metrics30d.overdraftCount} overdraft events in the last 30 days (${metrics7d.overdraftCount} in the last 7 days), exceeding the policy threshold of ${config.max30DayOverdraftCount}.",
        metricName = "Rolling30DayOverdraftCount",
        actualValue = s"${metrics30d.overdraftCount} overdrafts",
        thresholdValue = s"<= ${config.max30DayOverdraftCount} overdraft",
        evidenceIds = metrics30d.overdraftTxIds,
        counterfactualGuidance =
          "Maintain a positive account balance with at most 1 overdraft occurrence over a rolling 30-day period before re-applying.",
        isHardKnockout = false,
        scorePenalty = 110
      )
      RuleEvaluationTrace("RULE_LIQUIDITY_OVERDRAFT", "Short-Term Liquidity & Overdraft Check", passed = false, -110, Some(reason))
    else
      RuleEvaluationTrace("RULE_LIQUIDITY_OVERDRAFT", "Short-Term Liquidity & Overdraft Check", passed = true, +15, None)

  /** Rule 3: Real-Time High-Risk / Gambling Spend Velocity (30d Window) */
  private def evaluateHighRiskSpendVelocity(metrics30d: WindowMetrics): RuleEvaluationTrace =
    val ratio = metrics30d.highRiskToInflowRatio
    if ratio > config.max30DayHighRiskSpendRatio then
      val reason = ReasonCode(
        code = "RC_204_HIGH_RISK_SPEND_VELOCITY",
        title = "Elevated High-Risk or Speculative Outflow Velocity",
        regulatoryStatement =
          s"High-risk or gambling outflows over the past 30 days totaled ${fmt2(metrics30d.highRiskSpend)} EUR, representing ${fmt1(ratio * 100)}% of 30-day inflows and exceeding the ${fmt1(config.max30DayHighRiskSpendRatio * 100)}% policy limit.",
        metricName = "HighRiskSpendToInflowRatio30d",
        actualValue = s"${fmt1(ratio * 100)}% (${fmt2(metrics30d.highRiskSpend)} EUR)",
        thresholdValue = s"<= ${fmt1(config.max30DayHighRiskSpendRatio * 100)}%",
        evidenceIds = metrics30d.highRiskTxIds,
        counterfactualGuidance =
          s"Reduce speculative/high-risk outflows below ${fmt1(config.max30DayHighRiskSpendRatio * 100)}% of monthly inflows over a 30-day rolling window.",
        isHardKnockout = false,
        scorePenalty = 120
      )
      RuleEvaluationTrace("RULE_HIGH_RISK_VELOCITY", "30-Day High-Risk Outflow Velocity", passed = false, -120, Some(reason))
    else
      RuleEvaluationTrace("RULE_HIGH_RISK_VELOCITY", "30-Day High-Risk Outflow Velocity", passed = true, +10, None)

  /** Rule 4: Direct (1st-Hop) Restricted or Sanctioned Entity Exposure */
  private def evaluateDirectRestrictedExposure(state: CustomerSituationState): RuleEvaluationTrace =
    val directExposures = state.riskExposures.valuesIterator
      .filter(e => e.hopDistance == 1 && e.riskScore >= config.max1HopRiskScore)
      .toList
      .sortBy(-_.riskScore)

    directExposures.headOption match
      case Some(worst) =>
        val allEvidence = directExposures.flatMap(_.evidenceIds).distinct
        val reason = ReasonCode(
          code = "RC_301_DIRECT_RESTRICTED_COUNTERPARTY",
          title = "Direct Transaction with Restricted or Watchlisted Third Party",
          regulatoryStatement =
            s"Real-time transaction history indicates direct financial interaction with restricted entity '${worst.sourceEntityId}': ${worst.description}.",
          metricName = "DirectCounterpartyRiskScore",
          actualValue = f"${worst.riskScore}%.2f (1st-Hop Match: ${worst.sourceEntityId})",
          thresholdValue = f"< ${config.max1HopRiskScore}%.2f",
          evidenceIds = allEvidence,
          counterfactualGuidance =
            s"Compliance review required regarding transaction(s) ${allEvidence.mkString(", ")} with restricted entity '${worst.sourceEntityId}'.",
          isHardKnockout = true,
          scorePenalty = 300
        )
        RuleEvaluationTrace("RULE_RESTRICTED_1HOP", "1st-Hop Restricted Counterparty Screening", passed = false, -300, Some(reason))
      case None =>
        RuleEvaluationTrace("RULE_RESTRICTED_1HOP", "1st-Hop Restricted Counterparty Screening", passed = true, 0, None)

  /** Rule 5: Indirect (2nd-Hop) Peer Network Risk Contagion */
  private def evaluateSecondHopPeerContagion(state: CustomerSituationState): RuleEvaluationTrace =
    val peerExposures = state.riskExposures.valuesIterator
      .filter(e => e.hopDistance == 2 && e.riskScore >= config.max2HopRiskScore)
      .toList
      .sortBy(-_.riskScore)

    peerExposures.headOption match
      case Some(worst) =>
        val allEvidence = peerExposures.flatMap(_.evidenceIds).distinct
        val reason = ReasonCode(
          code = "RC_302_INDIRECT_NETWORK_RISK_CONTAGION",
          title = "Elevated Counterparty Network Risk Exposure (2nd-Hop Connection)",
          regulatoryStatement =
            s"Elevated risk contagion detected in active peer transaction network: ${worst.description} (attenuated network risk score ${f"${worst.riskScore}%.2f"} exceeds threshold ${f"${config.max2HopRiskScore}%.2f"}).",
          metricName = "PropagatedPeerNetworkRiskScore",
          actualValue = f"${worst.riskScore}%.2f (${worst.hopDistance}-Hop via ${worst.intermediaryPeerId.getOrElse("unknown")})",
          thresholdValue = f"< ${config.max2HopRiskScore}%.2f",
          evidenceIds = allEvidence,
          counterfactualGuidance =
            s"Risk score impacted by recent high-risk transfers with peer '${worst.intermediaryPeerId.getOrElse("unknown")}' (evidence: ${allEvidence.mkString(", ")}). Cease transfers with flagged counterparties or request manual compliance clearance.",
          isHardKnockout = false,
          scorePenalty = 150
        )
        RuleEvaluationTrace("RULE_NETWORK_CONTAGION_2HOP", "2nd-Hop Ego-Network Risk Contagion", passed = false, -150, Some(reason))
      case None =>
        RuleEvaluationTrace("RULE_NETWORK_CONTAGION_2HOP", "2nd-Hop Ego-Network Risk Contagion", passed = true, 0, None)

  /** Rule 6 (Section 5 & 14): Oracle Exadata Global Graph Risk Pattern (`NetworkRiskPatternDetected`) */
  private def evaluateGlobalGraphRiskPattern(state: CustomerSituationState): Option[RuleEvaluationTrace] =
    val globalExposures = state.riskExposures.valuesIterator
      .filter(e => e.hopDistance >= 3 && e.riskScore >= config.maxGlobalGraphRiskScore)
      .toList
      .sortBy(-_.riskScore)

    globalExposures.headOption.map { worst =>
      val allEvidence = globalExposures.flatMap(_.evidenceIds).distinct
      val reason = ReasonCode(
        code = "RC_303_GLOBAL_GRAPH_RISK_PATTERN",
        title = "Global Relationship Graph Risk Pattern Detected (Oracle Exadata SA_GRAPH)",
        regulatoryStatement =
          s"Global graph analysis identified structural risk pattern involving '${worst.sourceEntityId}': ${worst.description} (score ${f"${worst.riskScore}%.2f"} >= ${f"${config.maxGlobalGraphRiskScore}%.2f"}).",
        metricName = "GlobalGraphPatternRiskScore",
        actualValue = f"${worst.riskScore}%.2f (Global Pattern: ${worst.sourceEntityId})",
        thresholdValue = f"< ${config.maxGlobalGraphRiskScore}%.2f",
        evidenceIds = allEvidence,
        counterfactualGuidance =
          s"Global compliance review required for multi-account counterparty cluster '${worst.sourceEntityId}' (evidence: ${allEvidence.mkString(", ")}).",
        isHardKnockout = false,
        scorePenalty = 135
      )
      RuleEvaluationTrace("RULE_GLOBAL_GRAPH_PATTERN", "Oracle Exadata Global Graph Pattern Screening", passed = false, -135, Some(reason))
    }

  private def buildRegulatoryStatement(
      customerId: String,
      state: CustomerSituationState,
      projection: SituationProjection,
      application: LoanApplication,
      approved: Boolean,
      finalScore: Int,
      principalReasons: List[ReasonCode]
  ): String =
    val consistencyHeader =
      s"State Version  : v${state.stateVersion} | Policy: ${config.policyVersion} | Data Completeness: ${fmt1(BigDecimal(state.dataCompleteness.score * 100))}% (${state.dataCompleteness.level})\n" +
        s"Projection     : Future DTI=${fmt1(projection.futureDti * 100)}%, Expected Monthly Liquidity=${fmt2(projection.expectedMonthlyLiquidity)} EUR, Trend=${projection.projectedLiquidityTrend}"

    if approved then
      s"""STATEMENT OF CREDIT DECISION (APPROVED)
         |Application ID : ${application.applicationId}
         |Customer ID    : $customerId
         |Decision       : APPROVED for ${fmt2(application.requestedAmount)} EUR over ${application.termMonths} months
         |Monthly Payment: ${fmt2(application.estimatedMonthlyInstallment)} EUR
         |Composite Score: $finalScore / 850 (Minimum Required: ${config.minimumApprovalScore})
         |$consistencyHeader
         |Summary        : Customer real-time cashflow, verified income documents, and counterparty network checks met all credit policy requirements.""".stripMargin
    else
      val reasonsBlock = principalReasons.zipWithIndex
        .map { case (r, idx) =>
          s"""  ${idx + 1}. [${r.code}] ${r.title}
             |     - Explanation   : ${r.regulatoryStatement}
             |     - Metric        : ${r.metricName} | Actual: ${r.actualValue} | Required: ${r.thresholdValue}
             |     - Evidence IDs  : ${if r.evidenceIds.isEmpty then "N/A" else r.evidenceIds.mkString(", ")}
             |     - How to Resolve: ${r.counterfactualGuidance}""".stripMargin
        }
        .mkString("\n\n")

      s"""STATEMENT OF REASONS FOR CREDIT DENIAL (ADVERSE ACTION NOTICE)
         |Application ID : ${application.applicationId}
         |Customer ID    : $customerId
         |Decision       : DENIED (${fmt2(application.requestedAmount)} EUR for ${application.termMonths} months)
         |Composite Score: $finalScore / 850 (Minimum Required: ${config.minimumApprovalScore})
         |Policy Version : ${config.policyVersion}
         |$consistencyHeader
         |
         |Principal Reasons for Denial:
         |$reasonsBlock""".stripMargin
