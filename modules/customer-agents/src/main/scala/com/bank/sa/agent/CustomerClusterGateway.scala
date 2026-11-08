package com.bank.sa.agent

import com.bank.sa.api.{LoanOriginationDecisionDto, WorkflowTraceEventDto}
import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import org.apache.pekko.util.Timeout

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
 * Coordinates Pekko Cluster Sharding for `CustomerAgent` actors and records
 * a live telemetry log of shard routing, event persistence, and inter-agent risk propagation.
 */
class CustomerClusterGateway(
    system: ActorSystem[_],
    creditEngine: ExplainableCreditEngine
):
  private given Timeout          = Timeout(5.seconds)
  private given ExecutionContext = system.executionContext
  private val sharding           = ClusterSharding(system)

  private val seqCounter      = new AtomicLong(0L)
  private val knownCustomers  = new ConcurrentSkipListSet[String]()
  private val workflowTraces  = new ConcurrentLinkedDeque[WorkflowTraceEventDto]()
  private val recentDecisions = new ConcurrentLinkedDeque[LoanOriginationDecisionDto]()

  def recordTrace(
      stage: String,
      fromComponent: String,
      toComponent: String,
      eventType: String,
      summary: String,
      details: Map[String, String] = Map.empty,
      timestamp: Instant = Instant.now()
  ): Unit =
    val event = WorkflowTraceEventDto(
      sequenceNo = seqCounter.incrementAndGet(),
      stage = stage,
      fromComponent = fromComponent,
      toComponent = toComponent,
      eventType = eventType,
      summary = summary,
      details = details,
      timestamp = timestamp
    )
    workflowTraces.addFirst(event)
    while workflowTraces.size() > 150 do workflowTraces.removeLast()

  private val shardedRiskRouter: PeerRiskRouter = new PeerRiskRouter:
    override def sendRiskSignal(targetCustomerId: String, signal: PeerRiskSignal): Unit =
      knownCustomers.add(targetCustomerId)
      recordTrace(
        stage = "AGENT_TO_AGENT_IPC",
        fromComponent = s"CustomerAgent(${signal.fromCustomerId})",
        toComponent = s"CustomerAgent($targetCustomerId)",
        eventType = "PropagatePeerRisk (Hop=2)",
        summary =
          s"Actor '${signal.fromCustomerId}' propagated 2nd-hop risk signal '${signal.signalId}' (attenuated score=${f"${signal.attenuatedRiskScore}%.2f"}) to peer actor '$targetCustomerId'",
        details = Map(
          "signalId" -> signal.signalId,
          "originEntityId" -> signal.originCustomerId,
          "hopCount" -> signal.hopCount.toString,
          "attenuatedRiskScore" -> f"${signal.attenuatedRiskScore}%.2f",
          "reasonCode" -> signal.reasonCode,
          "rootEvidenceIds" -> signal.rootEvidenceIds.mkString(", ")
        ),
        timestamp = signal.emittedAt
      )
      val targetEntity = sharding.entityRefFor(CustomerAgent.TypeKey, targetCustomerId)
      targetEntity ! CustomerAgent.PropagatePeerRisk(signal, None)

  // Initialize the CustomerAgent shard region on this cluster node
  sharding.init(
    Entity(CustomerAgent.TypeKey) { entityContext =>
      CustomerAgent(
        customerId = entityContext.entityId,
        creditEngine = creditEngine,
        peerRiskRouter = shardedRiskRouter
      )
    }.withStopMessage(CustomerAgent.Passivate)
  )

  def entityFor(customerId: String): EntityRef[CustomerAgent.Command] =
    knownCustomers.add(customerId)
    sharding.entityRefFor(CustomerAgent.TypeKey, customerId)

  def ingestTransaction(tx: EnrichedTransaction): Future[CustomerAgent.IngestionAck] =
    val shardId = math.abs(tx.customerId.hashCode % 100)
    recordTrace(
      stage = "PEKKO_SHARD_ACTOR",
      fromComponent = s"ShardRegion(shard=$shardId)",
      toComponent = s"CustomerAgent(${tx.customerId})",
      eventType = s"IngestTransaction (${tx.direction})",
      summary =
        s"Routed ${tx.direction} transaction '${tx.txId}' (${tx.amount} ${tx.currency}, category=${tx.category}) to CustomerAgent(${tx.customerId})" +
          tx.restrictedTag.fold("")(t => s" [1st-Hop Watchlist Match: ${t.entityId}]"),
      details = Map(
        "txId" -> tx.txId,
        "customerId" -> tx.customerId,
        "counterpartyId" -> tx.counterpartyId,
        "direction" -> tx.direction.toString,
        "amount" -> s"${tx.amount} ${tx.currency}",
        "category" -> tx.category.toString,
        "overdraft" -> tx.resultsInOverdraft.toString,
        "restrictedMatch" -> tx.restrictedTag.map(t => s"${t.entityId} (${t.level})").getOrElse("None")
      ),
      timestamp = tx.timestamp
    )
    entityFor(tx.customerId).ask(replyTo => CustomerAgent.IngestTransaction(tx, replyTo))

  def ingestDocumentFact(fact: VerifiedDocumentFact): Future[CustomerAgent.IngestionAck] =
    val shardId = math.abs(fact.customerId.hashCode % 100)
    recordTrace(
      stage = "PEKKO_SHARD_ACTOR",
      fromComponent = s"ShardRegion(shard=$shardId)",
      toComponent = s"CustomerAgent(${fact.customerId})",
      eventType = s"IngestDocumentFact (${fact.factType})",
      summary =
        s"Updated verified document '${fact.docId}' (${fact.factType} = ${fact.numericValue.orElse(fact.textValue).getOrElse("")}) on CustomerAgent(${fact.customerId})",
      details = Map(
        "docId" -> fact.docId,
        "customerId" -> fact.customerId,
        "factType" -> fact.factType.toString,
        "issuer" -> fact.issuer
      ),
      timestamp = fact.verifiedAt
    )
    entityFor(fact.customerId).ask(replyTo => CustomerAgent.IngestDocumentFact(fact, replyTo))

  def evaluateLoan(application: LoanApplication): Future[LoanDecisionResponse] =
    entityFor(application.customerId)
      .ask[LoanDecisionResponse](replyTo => CustomerAgent.EvaluateLoan(application, replyTo))
      .map { response =>
        val dto = LoanOriginationDecisionDto.fromDomain(response, application.estimatedMonthlyInstallment)
        recentDecisions.addFirst(dto)
        while recentDecisions.size() > 50 do recentDecisions.removeLast()

        recordTrace(
          stage = "EXPLAINABLE_ENGINE",
          fromComponent = s"CustomerAgent(${application.customerId})",
          toComponent = "ExplainableCreditEngine",
          eventType = s"LoanDecisionAudited (${dto.status})",
          summary =
            s"Evaluated loan '${application.applicationId}' (${application.requestedAmount} EUR): ${dto.status} | Score: ${dto.creditScore}/850" +
              (if dto.adverseActionReasons.nonEmpty then
                 s" | Reasons: ${dto.adverseActionReasons.map(_.code).mkString(", ")}"
               else ""),
          details = Map(
            "applicationId" -> application.applicationId,
            "customerId" -> application.customerId,
            "decision" -> dto.status,
            "creditScore" -> s"${dto.creditScore} / 850",
            "reasonCodes" -> (if dto.adverseActionReasons.isEmpty then "NONE"
                              else dto.adverseActionReasons.map(_.code).mkString(", "))
          ),
          timestamp = application.requestedAt
        )
        response
      }

  def getSituationState(customerId: String): Future[CustomerSituationState] =
    entityFor(customerId).ask(replyTo => CustomerAgent.GetSituationState(replyTo))

  def resetAll(): Future[Unit] =
    val currentIds = knownCustomers.asScala.toList
    Future
      .traverse(currentIds) { cid =>
        sharding.entityRefFor(CustomerAgent.TypeKey, cid).ask[CustomerAgent.IngestionAck](replyTo => CustomerAgent.ResetState(replyTo))
      }
      .map { _ =>
        knownCustomers.clear()
        workflowTraces.clear()
        recentDecisions.clear()
        seqCounter.set(0L)
      }

  def getActiveCustomerIds: List[String] =
    knownCustomers.asScala.toList

  def getWorkflowTraces: List[WorkflowTraceEventDto] =
    workflowTraces.asScala.toList

  def getRecentDecisions: List[LoanOriginationDecisionDto] =
    recentDecisions.asScala.toList
