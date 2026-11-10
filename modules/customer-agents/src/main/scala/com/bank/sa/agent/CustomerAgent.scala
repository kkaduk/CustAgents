package com.bank.sa.agent

import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}

/**
 * Callback interface used by a `CustomerAgent` to asynchronously propagate
 * a `PeerRiskSignal` and Transactional Outbox entries to domain agents (`RiskSignalAgent`, `RelationshipAgent`,
 * `PortfolioRiskAgent`) and Oracle Exadata projections in the Pekko Cluster.
 */
trait PeerRiskRouter extends Serializable:
  def sendRiskSignal(targetCustomerId: String, signal: PeerRiskSignal): Unit

  def onEventProcessedByPlugin(
      customerId: String,
      observedEvent: AgentObservedEvent,
      result: PluginProcessingResult
  ): Unit =
    result.outboxEntries.flatMap(_.peerSignal).foreach { sig =>
      sendRiskSignal(sig.targetCustomerId, sig)
    }

object PeerRiskRouter:
  val NoOp: PeerRiskRouter = (_: String, _: PeerRiskSignal) => ()

/**
 * Digital Twin Actor (`CustomerAgent`) representing a single bank customer.
 * Identified uniquely by `CustomerAgent:CUST-12345` via Pekko Cluster Sharding (Section 2.1).
 *
 * Responsibilities:
 * 1. Routes every incoming observed event (`TransactionObserved`, `IncomeVerified`, `DebtUpdated`,
 *    `CounterpartyRiskReceived`, `ExternalDocumentReceived`, `NetworkRiskPatternDetected`) to its configured
 *    `SituationEventPlugin` via `EventPluginRegistry` (Section 2.1).
 * 2. Persists both the observed event and the resulting `SituationEvaluated` event (`Event 1006`) in the Event Journal.
 * 3. Guarantees Idempotency (including historical digest checks), Transactional Outbox persistence, and Situation Consistency (Section 10).
 * 4. Separates Level-3 `ProjectLoanSituation` (future scenario projection) from formal `EvaluateLoan` decisioning (Section 7).
 */
object CustomerAgent:

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("CustomerAgent")

  /** Attenuation factor applied per hop when propagating risk across customer connections. */
  val SecondHopAttenuationFactor: Double = 0.65

  // --- COMMAND PROTOCOL ---
  sealed trait Command extends CborSerializable

  final case class IngestionAck(customerId: String, referenceId: String) extends CborSerializable

  final case class IngestTransaction(
      tx: EnrichedTransaction,
      replyTo: ActorRef[IngestionAck]
  ) extends Command

  final case class IngestDocumentFact(
      fact: VerifiedDocumentFact,
      replyTo: ActorRef[IngestionAck]
  ) extends Command

  final case class IngestObservedEvent(
      event: AgentObservedEvent,
      replyTo: ActorRef[IngestionAck]
  ) extends Command

  final case class PropagatePeerRisk(
      signal: PeerRiskSignal,
      replyTo: Option[ActorRef[IngestionAck]] = None
  ) extends Command

  final case class IngestNetworkRiskPattern(
      pattern: NetworkRiskPatternDetected,
      replyTo: Option[ActorRef[IngestionAck]] = None
  ) extends Command

  final case class ConfirmOutboxDelivery(
      outboxId: String,
      replyTo: Option[ActorRef[IngestionAck]] = None
  ) extends Command

  final case class ProjectLoanSituation(
      application: LoanApplication,
      replyTo: ActorRef[SituationProjection]
  ) extends Command

  final case class EvaluateLoan(
      application: LoanApplication,
      replyTo: ActorRef[LoanDecisionResponse]
  ) extends Command

  final case class GetSituationState(
      replyTo: ActorRef[CustomerSituationState]
  ) extends Command

  final case class ResetState(
      replyTo: ActorRef[IngestionAck]
  ) extends Command

  case object Passivate extends Command

  // --- EVENT JOURNAL PROTOCOL (Section 2.1) ---
  sealed trait Event extends CborSerializable

  final case class ObservedEventPersisted(
      observed: AgentObservedEvent,
      handledByPluginId: String
  ) extends Event

  final case class SituationEvaluated(
      record: SituationEvaluatedRecord
  ) extends Event

  final case class OutboxDeliveryConfirmed(
      outboxId: String
  ) extends Event

  final case class TransactionIngested(tx: EnrichedTransaction) extends Event
  final case class DocumentFactIngested(fact: VerifiedDocumentFact) extends Event
  final case class PeerRiskSignalIngested(signal: PeerRiskSignal) extends Event
  final case class CustomerStateReset(customerId: String) extends Event
  final case class LoanDecisionAudited(
      decisionResponse: LoanDecisionResponse,
      frozenSituationState: CustomerSituationState
  ) extends Event

  // --- BEHAVIOR FACTORY ---
  def apply(
      customerId: String,
      creditEngine: ExplainableCreditEngine,
      peerRiskRouter: PeerRiskRouter = PeerRiskRouter.NoOp,
      pluginRegistry: EventPluginRegistry = EventPluginRegistry.default
  ): Behavior[Command] =
    Behaviors.setup { context =>
      EventSourcedBehavior[Command, Event, CustomerSituationState](
        persistenceId = PersistenceId(TypeKey.name, customerId),
        emptyState = CustomerSituationState.empty(customerId),
        commandHandler = (state, command) =>
          handleCommand(customerId, state, command, creditEngine, peerRiskRouter, pluginRegistry, context.log),
        eventHandler = (state, event) => handleEvent(state, event, pluginRegistry)
      ).withRetention(
        RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2)
      )
    }

  private def handleCommand(
      customerId: String,
      state: CustomerSituationState,
      command: Command,
      creditEngine: ExplainableCreditEngine,
      peerRiskRouter: PeerRiskRouter,
      pluginRegistry: EventPluginRegistry,
      log: org.slf4j.Logger
  ): Effect[Event, CustomerSituationState] =
    command match
      case IngestTransaction(tx, replyTo) =>
        val observed = TransactionObserved(
          eventId = tx.txId,
          customerId = customerId,
          tx = tx,
          occurredAt = tx.timestamp
        )
        processObservedEvent(customerId, state, observed, Some(replyTo), peerRiskRouter, pluginRegistry, log)

      case IngestDocumentFact(fact, replyTo) =>
        val observed = DocumentVerificationAgent.classifyToObservedEvent(fact)
        processObservedEvent(customerId, state, observed, Some(replyTo), peerRiskRouter, pluginRegistry, log)

      case IngestObservedEvent(observed, replyTo) =>
        processObservedEvent(customerId, state, observed, Some(replyTo), peerRiskRouter, pluginRegistry, log)

      case PropagatePeerRisk(signal, replyToOpt) =>
        if signal.hopCount > 2 || signal.originCustomerId == customerId then
          replyToOpt.foreach(_ ! IngestionAck(customerId, signal.signalId))
          Effect.none
        else
          val observed = CounterpartyRiskReceived(
            eventId = signal.signalId,
            customerId = customerId,
            signal = signal,
            occurredAt = signal.emittedAt
          )
          processObservedEvent(customerId, state, observed, replyToOpt, peerRiskRouter, pluginRegistry, log)

      case IngestNetworkRiskPattern(pattern, replyToOpt) =>
        val observed = GlobalRiskIntelligenceReceived(
          eventId = pattern.patternId,
          customerId = customerId,
          pattern = pattern,
          occurredAt = pattern.detectedAt
        )
        processObservedEvent(customerId, state, observed, replyToOpt, peerRiskRouter, pluginRegistry, log)

      case ConfirmOutboxDelivery(outboxId, replyToOpt) =>
        if state.pendingOutbox.contains(outboxId) then
          Effect
            .persist(OutboxDeliveryConfirmed(outboxId))
            .thenRun { _ =>
              replyToOpt.foreach(_ ! IngestionAck(customerId, outboxId))
            }
        else
          replyToOpt.foreach(_ ! IngestionAck(customerId, outboxId))
          Effect.none

      case ProjectLoanSituation(application, replyTo) =>
        val projection = creditEngine.projectSituation(state, application)
        Effect.reply(replyTo)(projection)

      case EvaluateLoan(application, replyTo) =>
        val decision = creditEngine.evaluate(state, application)
        Effect
          .persist(LoanDecisionAudited(decision, state))
          .thenReply(replyTo)(_ => decision)

      case GetSituationState(replyTo) =>
        Effect.reply(replyTo)(state)

      case ResetState(replyTo) =>
        Effect
          .persist(CustomerStateReset(customerId))
          .thenReply(replyTo)(_ => IngestionAck(customerId, "STATE-RESET"))

      case Passivate =>
        Effect.stop()

  private def processObservedEvent(
      customerId: String,
      state: CustomerSituationState,
      observed: AgentObservedEvent,
      replyToOpt: Option[ActorRef[IngestionAck]],
      peerRiskRouter: PeerRiskRouter,
      pluginRegistry: EventPluginRegistry,
      log: org.slf4j.Logger
  ): Effect[Event, CustomerSituationState] =
    // Section 10 Idempotency: reject duplicate events (including historical digest matches)
    if state.hasProcessedEvent(observed.eventId) then
      replyToOpt.foreach(_ ! IngestionAck(customerId, observed.eventId))
      Effect.none
    else
      val result = pluginRegistry.routeAndProcess(state, observed)
      val eventsToPersist = List(
        ObservedEventPersisted(observed, result.situationEvaluated.handledByPluginId),
        SituationEvaluated(result.situationEvaluated)
      )
      Effect
        .persist(eventsToPersist)
        .thenRun { (updatedState: CustomerSituationState) =>
          log.debug(
            "[CustomerAgent:{}] Processed '{}' ('{}') via plugin '{}' -> SituationEvaluated(v={}, status={})",
            customerId,
            observed.eventType,
            observed.eventId,
            result.situationEvaluated.handledByPluginId,
            updatedState.stateVersion,
            updatedState.situationSummary.financialSituationStatus
          )
          peerRiskRouter.onEventProcessedByPlugin(customerId, observed, result.copy(updatedState = updatedState))
          replyToOpt.foreach(_ ! IngestionAck(customerId, observed.eventId))
        }

  private def handleEvent(
      state: CustomerSituationState,
      event: Event,
      pluginRegistry: EventPluginRegistry
  ): CustomerSituationState =
    event match
      case ObservedEventPersisted(observed, _) =>
        pluginRegistry.routeAndProcess(state, observed).updatedState
      case SituationEvaluated(record) =>
        state.copy(
          stateVersion = record.stateVersion,
          ruleVersion = record.ruleVersion,
          situationSummary = record.summary,
          dataCompleteness = record.completeness,
          lastUpdatedAt = if record.evaluatedAt.isAfter(state.lastUpdatedAt) then record.evaluatedAt else state.lastUpdatedAt
        )
      case OutboxDeliveryConfirmed(outboxId) =>
        state.acknowledgeOutbox(outboxId)
      case TransactionIngested(tx) =>
        state.recordTransaction(tx)
      case DocumentFactIngested(fact) =>
        state.recordDocumentFact(fact)
      case PeerRiskSignalIngested(signal) =>
        state.recordPeerRiskSignal(signal)
      case CustomerStateReset(cid) =>
        CustomerSituationState.empty(cid)
      case _: LoanDecisionAudited =>
        state
