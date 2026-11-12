package com.bank.sa.engine

import com.bank.sa.domain.*
import java.time.{Duration, Instant}

/**
 * Policy configuration for the Pre-Execution Transaction Risk Decision Engine (`Doc/NowePotrzeby.md` Section 6).
 */
final case class TransactionRiskPolicyConfig(
    policyVersion: String = "SA-TX-RISK-POLICY-2026.11",
    reviewThresholdScore: Int = 35,
    blockThresholdScore: Int = 75,
    highValueTransferThreshold: BigDecimal = BigDecimal("10000.00"),
    maxAcceptableStateStalenessSeconds: Long = 3600L
) extends Serializable

/**
 * Pre-Execution Transaction Risk Decision Engine (`Doc/NowePotrzeby.md` Section 6):
 *
 * Evaluates a `ProposedPaymentTransfer` BEFORE payment execution using:
 * 1. The pre-computed `CustomerSituationState` maintained in memory by `CustomerAgent` (Apache Pekko),
 * 2. Fast O(1) lookup of recipient account history (`AccountVerificationResult`),
 * 3. Watchlist / restricted counterparty screening.
 *
 * Architectural principle (Section 6.2):
 * "Decyzja powinna wykorzystywać wcześniej przygotowany stan ryzyka, a nie wykonywać pełną analizę grafową
 *  od początku przy każdym przelewie."
 */
class TransactionRiskDecisionEngine(
    val config: TransactionRiskPolicyConfig = TransactionRiskPolicyConfig()
) extends Serializable:

  def evaluateTransferBeforeExecution(
      transfer: ProposedPaymentTransfer,
      customerStateOpt: Option[CustomerSituationState],
      accountVerification: AccountVerificationResult,
      restrictedCounterpartyTag: Option[RestrictedEntityTag],
      suspectedMuleEntities: Set[String] = Set.empty
  ): TransactionAuthorizationResponse =
    val startNanos = System.nanoTime()
    val now        = transfer.requestedAt

    val fallbackMode = customerStateOpt.isEmpty
    val state        = customerStateOpt.getOrElse(CustomerSituationState.empty(transfer.customerId))

    // Own-account internal transfer check (Section 3.3)
    val isOwnAccountTransfer =
      transfer.customerId == transfer.toCounterpartyId ||
        state.ownedAccountIds.contains(transfer.toAccountId)

    val rule1AccountHistory = evaluateAccountHistory(transfer, accountVerification, isOwnAccountTransfer)
    val rule2MuleSuspect    = evaluateSuspectedMuleAccount(transfer, state, accountVerification, suspectedMuleEntities)
    val rule3Restricted     = evaluateRestrictedCounterparty(transfer, state, restrictedCounterpartyTag)
    val rule4EmployeePhone  = evaluateEmployeeCustomerPhoneLink(transfer, state)

    val traces = List(rule1AccountHistory, rule2MuleSuspect, rule3Restricted, rule4EmployeePhone)

    val rawRiskScore = math.max(0, math.min(100, traces.map(t => math.max(0, -t.pointsDelta)).sum))
    val reasons      = traces.flatMap(_.reasonCode).sortBy(r => (!r.isHardKnockout, -r.scorePenalty))
    val hasHardBlock = reasons.exists(_.isHardKnockout)

    val decision: TransactionAuthorizationDecision =
      if isOwnAccountTransfer && !hasHardBlock then
        TransactionAuthorizationDecision.ALLOW
      else if hasHardBlock || rawRiskScore >= config.blockThresholdScore then
        TransactionAuthorizationDecision.BLOCK
      else if fallbackMode || rawRiskScore >= config.reviewThresholdScore then
        TransactionAuthorizationDecision.REVIEW_HOLD
      else
        TransactionAuthorizationDecision.ALLOW

    val directive = decision match
      case TransactionAuthorizationDecision.ALLOW       => "EXECUTE_IMMEDIATELY"
      case TransactionAuthorizationDecision.REVIEW_HOLD => "HOLD_FOR_ANALYST_AND_STEP_UP_VERIFICATION"
      case TransactionAuthorizationDecision.BLOCK       => "REJECT_AND_BLOCK_OUTFLOW"

    val freshnessMs =
      if state.lastUpdatedAt.isAfter(Instant.EPOCH) then
        math.max(0L, Duration.between(state.lastUpdatedAt, now).toMillis)
      else 0L

    val elapsedMs = math.max(1L, (System.nanoTime() - startNanos) / 1_000_000L)

    val statement = buildExplainableStatement(
      transfer = transfer,
      decision = decision,
      riskScore = rawRiskScore,
      reasons = reasons,
      accountVerification = accountVerification,
      state = state,
      fallbackMode = fallbackMode,
      directive = directive,
      responseTimeMs = elapsedMs
    )

    TransactionAuthorizationResponse(
      transferId = transfer.transferId,
      customerId = transfer.customerId,
      fromAccountId = transfer.fromAccountId,
      toAccountId = transfer.toAccountId,
      toCounterpartyId = transfer.toCounterpartyId,
      amount = transfer.amount,
      currency = transfer.currency,
      decision = decision,
      riskScore = rawRiskScore,
      reviewThresholdScore = config.reviewThresholdScore,
      blockThresholdScore = config.blockThresholdScore,
      policyVersion = config.policyVersion,
      ruleTraces = traces,
      principalReasons = reasons,
      statementOfReasons = statement,
      accountVerification = accountVerification,
      stateVersion = state.stateVersion,
      stateFreshnessMs = freshnessMs,
      responseTimeMs = elapsedMs,
      fallbackModeUsed = fallbackMode,
      paymentRailDirective = directive,
      evaluatedAt = now
    )

  /**
   * Rule 1: Recipient Account History (`UNKNOWN_ACCOUNT_HISTORY`, Section 4 & Section 6.3).
   * Important business rule (Section 4 & 6.3):
   * "Brak wcześniejszej historii rachunku jest sygnałem ryzyka kradzieży tożsamości.
   *  Nie oznacza jednak automatycznej odmowy wykonania transakcji."
   */
  private def evaluateAccountHistory(
      transfer: ProposedPaymentTransfer,
      verification: AccountVerificationResult,
      isOwnAccountTransfer: Boolean
  ): RuleEvaluationTrace =
    if isOwnAccountTransfer then
      RuleEvaluationTrace("TX_RULE_ACCOUNT_HISTORY", "Recipient Account History Check", passed = true, 0, None)
    else if !verification.knownInBank then
      val isMicroVerification = transfer.amount <= BigDecimal("5.00") || transfer.isVerificationOnePlnTransfer
      val isHighValue         = transfer.amount >= config.highValueTransferThreshold
      // Unknown account history alone never triggers a hard block (>= 75):
      // - For 1 PLN verification transfer: +20 risk points (< 35 review threshold -> ALLOW with recorded signal, unless combined with other risks)
      // - For standard transfer: +35 risk points (triggers REVIEW_HOLD)
      // - For high-value transfer (>= 10,000): +45 risk points (triggers REVIEW_HOLD, or BLOCK if combined with employee link / mule signal)
      val penalty =
        if isMicroVerification then 20
        else if isHighValue then 45
        else 35

      val reason = ReasonCode(
        code = "UNKNOWN_ACCOUNT_HISTORY",
        title = "Brak wcześniejszej historii rachunku odbiorcy (Nowy / nieobserwowany rachunek)",
        regulatoryStatement =
          s"Rachunek odbiorcy '${transfer.toAccountId}' nie występował wcześniej w historii obserwowanej przez bank (${verification.assessment}). Brak wcześniejszej historii stanowi sygnał ostrzegawczy, lecz sam w sobie nie oznacza automatycznej blokady.",
        metricName = "AccountKnownInBank",
        actualValue = s"knownInBank=NIE (kwota=${transfer.amount} ${transfer.currency})",
        thresholdValue = "knownInBank=TAK (lub potwierdzenie tożsamości odbiorcy)",
        evidenceIds = List(transfer.transferId, s"ACC:${transfer.toAccountId}"),
        counterfactualGuidance =
          "Zweryfikuj tożsamość odbiorcy i źródło numeru rachunku (np. potwierdzenie telefoniczne / silne uwierzytelnienie 2FA).",
        isHardKnockout = false,
        scorePenalty = penalty
      )
      RuleEvaluationTrace("TX_RULE_ACCOUNT_HISTORY", "Recipient Account History Check", passed = false, -penalty, Some(reason))
    else if verification.highRiskConnections.nonEmpty then
      val reason = ReasonCode(
        code = "UNKNOWN_ACCOUNT_HISTORY",
        title = "Rachunek odbiorcy powiązany wcześniej z podejrzanymi transakcjami",
        regulatoryStatement =
          s"Rachunek odbiorcy '${transfer.toAccountId}' występował wcześniej w transakcjach podwyższonego ryzyka (${verification.highRiskConnections.mkString(", ")}).",
        metricName = "AccountHighRiskHistory",
        actualValue = s"highRiskConnections=${verification.highRiskConnections.size}",
        thresholdValue = "0 powiązań wysokiego ryzyka",
        evidenceIds = (transfer.transferId :: verification.suspiciousTxIds).distinct,
        counterfactualGuidance =
          "Wymagana weryfikacja analityka AML/Fraud przed realizacją przelewu na ten rachunek.",
        isHardKnockout = false,
        scorePenalty = 50
      )
      RuleEvaluationTrace("TX_RULE_ACCOUNT_HISTORY", "Recipient Account History Check", passed = false, -50, Some(reason))
    else
      RuleEvaluationTrace("TX_RULE_ACCOUNT_HISTORY", "Recipient Account History Check", passed = true, 0, None)

  /**
   * Rule 2: Suspected Mule Account / Money Movement Pattern (`SUSPECTED_MULE_ACCOUNT`, Section 3 & Section 6.3).
   */
  private def evaluateSuspectedMuleAccount(
      transfer: ProposedPaymentTransfer,
      state: CustomerSituationState,
      verification: AccountVerificationResult,
      suspectedMuleEntities: Set[String]
  ): RuleEvaluationTrace =
    val mulePatternsInState = state.riskExposures.valuesIterator.filter { exp =>
      exp.reasonCode == "SUSPECTED_MULE_ACCOUNT" ||
      exp.description.contains("RapidPassThrough") ||
      exp.description.contains("FanInFanOut") ||
      exp.description.contains("MultiHopMoneyMovement") ||
      exp.description.contains("CircularTransfers")
    }.toList

    val recipientIsMule =
      suspectedMuleEntities.contains(transfer.toCounterpartyId) ||
        suspectedMuleEntities.contains(transfer.toAccountId) ||
        verification.riskSignalCode.contains("SUSPECTED_MULE_ACCOUNT")

    if recipientIsMule || mulePatternsInState.nonEmpty then
      val evidence = (transfer.transferId :: mulePatternsInState.flatMap(_.evidenceIds)).distinct
      val desc =
        if recipientIsMule then
          s"Odbiorca '${transfer.toCounterpartyId}' (rachunek '${transfer.toAccountId}') został zidentyfikowany w analizie przepływów jako podejrzane konto słup."
        else
          mulePatternsInState.map(_.description).mkString("; ")

      val isHardBlock = recipientIsMule || transfer.amount >= BigDecimal("1000.00")
      val penalty     = if isHardBlock then 80 else 55

      val reason = ReasonCode(
        code = "SUSPECTED_MULE_ACCOUNT",
        title = "Podejrzenie wykorzystania konta słupa (Mule Account / Rapid Pass-Through / Multi-Hop)",
        regulatoryStatement =
          s"Wykryto podejrzany wzorzec przepływów środków charakterystyczny dla konta słupa: $desc",
        metricName = "MuleNetworkPatternMatch",
        actualValue = s"MATCH (odbiorca=${transfer.toCounterpartyId}, wzorce=${mulePatternsInState.size})",
        thresholdValue = "Brak udziału w łańcuchu kont słupów",
        evidenceIds = evidence,
        counterfactualGuidance =
          "Zatrzymaj przelew i przekaż sprawę do zespołu AML/Anti-Fraud w celu zbadania łańcucha pochodzenia i transferu środków.",
        isHardKnockout = isHardBlock,
        scorePenalty = penalty
      )
      RuleEvaluationTrace("TX_RULE_MULE_DETECTION", "Suspected Mule Account & Flow Pattern Check", passed = false, -penalty, Some(reason))
    else
      RuleEvaluationTrace("TX_RULE_MULE_DETECTION", "Suspected Mule Account & Flow Pattern Check", passed = true, 0, None)

  /**
   * Rule 3: Restricted Counterparty / Watchlist Screening (`RESTRICTED_COUNTERPARTY`, Section 6.1 & 6.3).
   */
  private def evaluateRestrictedCounterparty(
      transfer: ProposedPaymentTransfer,
      state: CustomerSituationState,
      restrictedTagOpt: Option[RestrictedEntityTag]
  ): RuleEvaluationTrace =
    restrictedTagOpt match
      case Some(tag) =>
        val reason = ReasonCode(
          code = "RESTRICTED_COUNTERPARTY",
          title = "Powiązanie odbiorcy z listą restrykcyjną / sankcyjną",
          regulatoryStatement =
            s"Odbiorca przelewu '${tag.entityId}' znajduje się na liście restrykcyjnej '${tag.listSource}' (${tag.level}: ${tag.reasonDescription}).",
          metricName = "RestrictedWatchlistMatch",
          actualValue = s"${tag.entityId} (${tag.level})",
          thresholdValue = "Brak wpisu na listach sankcyjnych i fraudowych",
          evidenceIds = List(transfer.transferId, tag.entityId),
          counterfactualGuidance =
            "Przelew do podmiotu objętego restrykcjami podlega natychmiastowej blokadzie zgodnie z polityką AML/CFT.",
          isHardKnockout = true,
          scorePenalty = 100
        )
        RuleEvaluationTrace("TX_RULE_RESTRICTED_COUNTERPARTY", "Restricted Watchlist Screening", passed = false, -100, Some(reason))

      case None =>
        val directOrPeerRestricted = state.riskExposures.valuesIterator
          .filter(e => e.reasonCode.startsWith("RC_RESTRICTED") || e.reasonCode.startsWith("RC_302_PEER_LINK"))
          .toList
          .sortBy(-_.riskScore)

        directOrPeerRestricted.headOption match
          case Some(exp) if exp.sourceEntityId == transfer.toCounterpartyId || exp.riskScore >= 0.85 =>
            val reason = ReasonCode(
              code = "RESTRICTED_COUNTERPARTY",
              title = "Bezpośrednia ekspozycja na podmiot z listy restrykcyjnej",
              regulatoryStatement =
                s"Aktualna sytuacja klienta zawiera aktywną ekspozycję na podmiot restrykcyjny '${exp.sourceEntityId}': ${exp.description}.",
              metricName = "CustomerRestrictedExposureScore",
              actualValue = f"${exp.riskScore}%.2f (${exp.sourceEntityId})",
              thresholdValue = "< 0.85",
              evidenceIds = (transfer.transferId :: exp.evidenceIds).distinct,
              counterfactualGuidance =
                "Wymagana autoryzacja Compliance dla klienta posiadającego bezpośrednią ekspozycję sankcyjną.",
              isHardKnockout = exp.sourceEntityId == transfer.toCounterpartyId,
              scorePenalty = if exp.sourceEntityId == transfer.toCounterpartyId then 95 else 45
            )
            RuleEvaluationTrace(
              "TX_RULE_RESTRICTED_COUNTERPARTY",
              "Restricted Watchlist Screening",
              passed = false,
              -reason.scorePenalty,
              Some(reason)
            )
          case _ =>
            RuleEvaluationTrace("TX_RULE_RESTRICTED_COUNTERPARTY", "Restricted Watchlist Screening", passed = true, 0, None)

  /**
   * Rule 4: Employee–Customer Shared Phone Number (`EMPLOYEE_CUSTOMER_PHONE_MATCH`, Section 2 & Section 6.3).
   * Important business rule (Section 2):
   * "Samo współdzielenie numeru powinno być traktowane jako sygnał wymagający wyjaśnienia, a nie dowód nadużycia."
   */
  private def evaluateEmployeeCustomerPhoneLink(
      transfer: ProposedPaymentTransfer,
      state: CustomerSituationState
  ): RuleEvaluationTrace =
    val empExposures = state.riskExposures.valuesIterator
      .filter(_.reasonCode == "EMPLOYEE_CUSTOMER_PHONE_MATCH")
      .toList

    empExposures.headOption match
      case Some(exp) =>
        // Alone: +40 risk score -> triggers REVIEW_HOLD (>= 35), NOT automatic BLOCK (< 75).
        // When combined with UNKNOWN_ACCOUNT_HISTORY (+35/+45) or SUSPECTED_MULE_ACCOUNT (+55/+80), total >= 75 -> triggers BLOCK!
        val penalty = 40
        val reason = ReasonCode(
          code = "EMPLOYEE_CUSTOMER_PHONE_MATCH",
          title = "Wspólny numer telefonu pracownika banku i obsługiwanego klienta",
          regulatoryStatement =
            s"W kartotece klienta wykryto powiązanie telefoniczne z pracownikiem banku: ${exp.description}. Sygnał wymaga wyjaśnienia przed autoryzacją niestandardowych transferów.",
          metricName = "EmployeeCustomerPhoneCollision",
          actualValue = s"MATCH (${exp.sourceEntityId} <-> ${state.currentPhoneNumber.getOrElse("phone")})",
          thresholdValue = "Brak nieautoryzowanego współdzielenia danych kontaktowych z pracownikiem obsługującym",
          evidenceIds = (transfer.transferId :: exp.evidenceIds).distinct,
          counterfactualGuidance =
            "Zweryfikuj historię zmiany numeru telefonu klienta oraz zasadność przelewu z niezależnym inspektorem bezpieczeństwa.",
          isHardKnockout = false,
          scorePenalty = penalty
        )
        RuleEvaluationTrace("TX_RULE_EMPLOYEE_PHONE_MATCH", "Employee–Customer Contact Link Screening", passed = false, -penalty, Some(reason))

      case None =>
        RuleEvaluationTrace("TX_RULE_EMPLOYEE_PHONE_MATCH", "Employee–Customer Contact Link Screening", passed = true, 0, None)

  private def buildExplainableStatement(
      transfer: ProposedPaymentTransfer,
      decision: TransactionAuthorizationDecision,
      riskScore: Int,
      reasons: List[ReasonCode],
      accountVerification: AccountVerificationResult,
      state: CustomerSituationState,
      fallbackMode: Boolean,
      directive: String,
      responseTimeMs: Long
  ): String =
    val header =
      s"""PRE-EXECUTION TRANSACTION RISK DECISION (${decision.toString})
         |Transfer ID    : ${transfer.transferId}
         |Customer ID    : ${transfer.customerId} (State v${state.stateVersion}${if fallbackMode then " [FALLBACK MODE]" else ""})
         |From -> To Acc : ${transfer.fromAccountId.getOrElse("DEFAULT-ACC")} -> ${transfer.toAccountId} (${transfer.toCounterpartyId})
         |Amount         : ${transfer.amount} ${transfer.currency}
         |Risk Score     : $riskScore / 100 (Review >= ${config.reviewThresholdScore}, Block >= ${config.blockThresholdScore})
         |Account Known  : ${if accountVerification.knownInBank then s"TAK (firstSeen=${accountVerification.firstSeenAt.map(_.toString).getOrElse("N/A")}, txCount=${accountVerification.transactionCount})" else "NIE (Nowy / nieobserwowany rachunek)"}
         |Rail Directive : $directive (Latency: ${responseTimeMs}ms)""".stripMargin

    if reasons.isEmpty then
      s"""$header
         |Summary        : Przelew pozytywnie przeszedł weryfikację historii rachunku, list restrykcyjnych, wzorców kont słupów oraz powiązań wewnętrznych.""".stripMargin
    else
      val reasonsText = reasons.zipWithIndex
        .map { case (r, idx) =>
          s"""  ${idx + 1}. [${r.code}] ${r.title} (Penalty: +${r.scorePenalty} pkt)
             |     - Wyjaśnienie : ${r.regulatoryStatement}
             |     - Metryka     : ${r.metricName} = ${r.actualValue} (Wymagane: ${r.thresholdValue})
             |     - Dowody      : ${r.evidenceIds.mkString(", ")}
             |     - Zalecenie   : ${r.counterfactualGuidance}""".stripMargin
        }
        .mkString("\n\n")
      s"""$header
         |
         |Wykryte Sygnały Ryzyka (Explainable Audit Trail):
         |$reasonsText""".stripMargin

