package com.bank.sa.api

import com.bank.sa.domain.*
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}

import java.time.{Duration, Instant}
import scala.reflect.ClassTag

// --- REQUEST DTOs ---

final case class LoanOriginationRequest(
    applicationId: String,
    customerId: String,
    requestedAmount: BigDecimal,
    termMonths: Int,
    annualInterestRate: BigDecimal,
    requestedAt: Option[Instant] = None
):
  def toDomain: LoanApplication =
    LoanApplication(
      applicationId = applicationId,
      customerId = customerId,
      requestedAmount = requestedAmount,
      termMonths = termMonths,
      annualInterestRate = annualInterestRate,
      requestedAt = Option(requestedAt).flatten.getOrElse(Instant.now())
    )

final case class DocumentFactUploadRequest(
    docId: String,
    factType: String,
    numericValue: Option[BigDecimal],
    textValue: Option[String],
    issuer: String,
    verifiedAt: Option[Instant] = None
):
  def toDomain(customerId: String): VerifiedDocumentFact =
    VerifiedDocumentFact(
      docId = docId,
      customerId = customerId,
      factType = DocumentFactType.valueOf(factType),
      numericValue = Option(numericValue).flatten,
      textValue = Option(textValue).flatten,
      issuer = Option(issuer).getOrElse("Registry"),
      verifiedAt = Option(verifiedAt).flatten.getOrElse(Instant.now())
    )

final case class RawTransactionIngestRequest(
    txId: String,
    fromCustomerId: String,
    toCounterpartyId: String,
    counterpartyIsBankClient: Boolean,
    amount: BigDecimal,
    currency: String,
    category: String,
    resultsInOverdraft: Boolean,
    timestamp: Option[Instant] = None
):
  def toDomain: RawTransaction =
    RawTransaction(
      txId = txId,
      fromCustomerId = fromCustomerId,
      toCounterpartyId = toCounterpartyId,
      counterpartyIsBankClient = counterpartyIsBankClient,
      amount = amount,
      currency = Option(currency).getOrElse("EUR"),
      category = TransactionCategory.valueOf(category),
      resultsInOverdraft = resultsInOverdraft,
      timestamp = Option(timestamp).flatten.getOrElse(Instant.now())
    )

// --- RESPONSE DTOs ---

final case class ReasonCodeDto(
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
)

object ReasonCodeDto:
  def fromDomain(rc: ReasonCode): ReasonCodeDto =
    ReasonCodeDto(
      code = rc.code,
      title = rc.title,
      regulatoryStatement = rc.regulatoryStatement,
      metricName = rc.metricName,
      actualValue = rc.actualValue,
      thresholdValue = rc.thresholdValue,
      evidenceIds = rc.evidenceIds,
      counterfactualGuidance = rc.counterfactualGuidance,
      isHardKnockout = rc.isHardKnockout,
      scorePenalty = rc.scorePenalty
    )

final case class RuleTraceDto(
    ruleId: String,
    ruleName: String,
    passed: Boolean,
    pointsDelta: Int
)

final case class LoanOriginationDecisionDto(
    applicationId: String,
    customerId: String,
    status: String,
    approvedAmount: Option[BigDecimal],
    monthlyInstallment: BigDecimal,
    creditScore: Int,
    minimumRequiredScore: Int,
    policyVersion: String,
    ruleTraces: List[RuleTraceDto],
    adverseActionReasons: List[ReasonCodeDto],
    statementOfReasons: String,
    evaluatedAt: Instant
)

object LoanOriginationDecisionDto:
  def fromDomain(response: LoanDecisionResponse, requestedInstallment: BigDecimal): LoanOriginationDecisionDto =
    val (status, approvedAmt, installment) = response.outcome match
      case DecisionOutcome.Approved(amt, _, _, inst, _) =>
        ("APPROVED", Some(amt), inst)
      case DecisionOutcome.Denied(_, _, _) =>
        ("DENIED", None, requestedInstallment)

    LoanOriginationDecisionDto(
      applicationId = response.applicationId,
      customerId = response.customerId,
      status = status,
      approvedAmount = approvedAmt,
      monthlyInstallment = installment,
      creditScore = response.auditTrail.finalCreditScore,
      minimumRequiredScore = response.auditTrail.minimumRequiredScore,
      policyVersion = response.auditTrail.policyVersion,
      ruleTraces = response.auditTrail.ruleTraces.map(t =>
        RuleTraceDto(t.ruleId, t.ruleName, t.passed, t.pointsDelta)
      ),
      adverseActionReasons = response.auditTrail.principalAdverseReasons.map(ReasonCodeDto.fromDomain),
      statementOfReasons = response.auditTrail.statementOfReasons,
      evaluatedAt = response.evaluatedAt
    )

final case class RiskExposureDto(
    exposureKey: String,
    sourceEntityId: String,
    intermediaryPeerId: Option[String],
    hopDistance: Int,
    riskScore: Double,
    reasonCode: String,
    description: String,
    evidenceIds: List[String]
)

final case class CustomerSituationSnapshotDto(
    customerId: String,
    inflow30d: BigDecimal,
    outflow30d: BigDecimal,
    netCashflow30d: BigDecimal,
    highRiskSpend30d: BigDecimal,
    overdraftCount30d: Int,
    verifiedFacts: Map[String, String],
    connectedBankPeers: List[String],
    maxNetworkRiskScore: Double,
    riskExposures: List[RiskExposureDto],
    lastUpdatedAt: Instant
)

object CustomerSituationSnapshotDto:
  def fromDomain(state: CustomerSituationState, referenceTime: Instant): CustomerSituationSnapshotDto =
    val ref        = if state.lastUpdatedAt.isAfter(Instant.EPOCH) then state.lastUpdatedAt else referenceTime
    val metrics30d = state.cashflow.metricsForWindow(ref, Duration.ofDays(30))
    val factsMap = state.verifiedFacts.map { case (k, v) =>
      val valueStr = v.numericValue.map(_.toString).orElse(v.textValue).getOrElse("")
      k.toString -> s"$valueStr (docId=${v.docId}, issuer=${v.issuer})"
    }
    val exposures = state.riskExposures.valuesIterator.toList.map { e =>
      RiskExposureDto(
        exposureKey = e.exposureKey,
        sourceEntityId = e.sourceEntityId,
        intermediaryPeerId = e.intermediaryPeerId,
        hopDistance = e.hopDistance,
        riskScore = e.riskScore,
        reasonCode = e.reasonCode,
        description = e.description,
        evidenceIds = e.evidenceIds
      )
    }

    CustomerSituationSnapshotDto(
      customerId = state.customerId,
      inflow30d = metrics30d.totalInflow,
      outflow30d = metrics30d.totalOutflow,
      netCashflow30d = metrics30d.netCashflow,
      highRiskSpend30d = metrics30d.highRiskSpend,
      overdraftCount30d = metrics30d.overdraftCount,
      verifiedFacts = factsMap,
      connectedBankPeers = state.connectedBankPeers.map(_.counterpartyId),
      maxNetworkRiskScore = state.maxNetworkRiskScore,
      riskExposures = exposures,
      lastUpdatedAt = state.lastUpdatedAt
    )

final case class IngestionAckDto(
    customerId: String,
    referenceId: String,
    status: String = "PERSISTED"
)

final case class HealthStatusDto(
    service: String,
    status: String,
    clusterMemberStatus: String
)

/**
 * Represents a single internal architectural step or inter-agent message recorded for business/technical inspection.
 */
final case class WorkflowTraceEventDto(
    sequenceNo: Long,
    stage: String,      // e.g. "FLINK_ENRICHMENT", "PEKKO_SHARD_ACTOR", "AGENT_TO_AGENT_IPC", "EXPLAINABLE_ENGINE"
    fromComponent: String,
    toComponent: String,
    eventType: String,
    summary: String,
    details: Map[String, String],
    timestamp: Instant
)

final case class WatchlistEntryDto(
    entityId: String,
    level: String,
    baseRiskWeight: Double,
    listSource: String,
    reasonDescription: String
)

object WatchlistEntryDto:
  def fromDomain(tag: RestrictedEntityTag): WatchlistEntryDto =
    WatchlistEntryDto(
      entityId = tag.entityId,
      level = tag.level.toString,
      baseRiskWeight = tag.level.baseRiskWeight,
      listSource = tag.listSource,
      reasonDescription = tag.reasonDescription
    )

final case class SystemOverviewDto(
    activeCustomerIds: List[String],
    customers: List[CustomerSituationSnapshotDto],
    watchlist: List[WatchlistEntryDto],
    workflowTraces: List[WorkflowTraceEventDto],
    recentDecisions: List[LoanOriginationDecisionDto]
)

/**
 * Pekko HTTP JSON marshalling and unmarshalling backed by Jackson (`DefaultScalaModule` + `JavaTimeModule`).
 */
object JacksonJsonSupport:
  val mapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  given [T](using ct: ClassTag[T]): FromEntityUnmarshaller[T] =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(ContentTypes.`application/json`)
      .map { bytes =>
        mapper.readValue(bytes.toArray, ct.runtimeClass.asInstanceOf[Class[T]])
      }

  given [T]: ToEntityMarshaller[T] =
    Marshaller.withFixedContentType(ContentTypes.`application/json`) { value =>
      val jsonBytes = mapper.writeValueAsBytes(value)
      HttpEntity(ContentTypes.`application/json`, jsonBytes)
    }

