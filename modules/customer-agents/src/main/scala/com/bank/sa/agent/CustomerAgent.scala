package com.bank.sa.agent

import com.bank.sa.domain.*
import com.bank.sa.engine.ExplainableCreditEngine
import com.bank.sa.llm.CustomerSituationLlmSummarizer
import org.apache.pekko.actor.NotInfluenceReceiveTimeout
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import org.apache.pekko.persistence.typed.{PersistenceId, RecoveryCompleted}
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/**
 * Callback interface used by a `CustomerAgent` to asynchronously propagate
 * a `PeerRiskSignal` and Transactional Outbox entries to domain agents (`RiskSignalAgent`, `RelationshipAgent`,
 * `PortfolioRiskAgent`), LLM summary telemetry, lifecycle events (wake-up / passivation),
 * and Oracle Exadata projections in the Pekko Cluster.
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

  /**
   * Re-dispatches unacknowledged Transactional Outbox entries upon actor recovery or periodic outbox sweep (Roadmap 9.3).
   */
  def retryPendingOutbox(
      customerId: String,
      pendingEntries: Iterable[OutboxEntry]
  ): Unit =
    pendingEntries.flatMap(_.peerSignal).foreach { sig =>
      sendRiskSignal(sig.targetCustomerId, sig)
    }

  def onLlmSummaryCompleted(
      customerId: String,
      summary: LlmSituationSummary,
      updatedState: CustomerSituationState
  ): Unit = ()

  def onAgentWokenUp(
      customerId: String,
      wakeUpReason: String,
      stateVersion: Long
  ): Unit = ()

  def onAgentPassivated(
      customerId: String,
      stateVersion: Long
  ): Unit = ()

object PeerRiskRouter:
  val NoOp: PeerRiskRouter = (_: String, _: PeerRiskSignal) => ()

/**
 * Digital Twin Actor (`CustomerAgent`) representing a single bank customer.
 * Identified uniquely by `CustomerAgent:CUST-12345` via Pekko Cluster Sharding (Section 2.1).
 *
 * Responsibilities:
 * 1. Routes every incoming observed event (`TransactionObserved`, `IncomeVerified`, `DebtUpdated`,
 *    `CounterpartyRiskReceived`, `ExternalDocumentReceived`, `NetworkRiskPatternDetected`,
 *    `CustomerContactChanged`, `AccountHistoryRiskDetected`) to its configured
 *    `SituationEventPlugin` via `EventPluginRegistry` (Section 2.1).
 * 2. Persists both the observed event and the resulting `SituationEvaluated` event (`Event 1006`) in the Event Journal.
 * 3. Guarantees Idempotency (including historical digest checks), Transactional Outbox persistence, and Situation Consistency (Section 10).
 * 4. Separates Level-3 `ProjectLoanSituation` (future scenario projection) from formal `EvaluateLoan` decisioning (Section 7).
 * 5. Cyclically (after `llmTxCountThreshold` transactions OR `llmSummaryInterval` time) synthesizes a natural-language
 *    Customer Situation Summary via `CustomerSituationLlmSummarizer` (`ai-models` module).
 * 6. Wakes up automatically when a transaction arrives from Kafka OR when queried via REST for customer state/description.
 * 7. When idle (`RequestPassivate` / `Passivate`), cancels all timers and stops so no LLM summaries are executed while passivated.
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

  /**
   * Wakes up the `CustomerAgent` (if passivated) in response to a REST query for the customer's situation description,
   * optionally triggering an immediate LLM situation summary if none exists or if `forceRefresh = true`.
   */
  final case class GetOrGenerateSituationDescription(
      forceRefresh: Boolean = false,
      wakeUpSource: String = "REST_QUERY",
      replyTo: ActorRef[CustomerSituationState]
  ) extends Command

  final case class RecordLlmSituationSummary(
      summary: LlmSituationSummary,
      replyTo: Option[ActorRef[CustomerSituationState]] = None
  ) extends Command

  final case class LlmSummaryFailed(
      reason: String,
      fallbackState: CustomerSituationState,
      replyTo: Option[ActorRef[CustomerSituationState]] = None
  ) extends Command

  final case class ResetState(
      replyTo: ActorRef[IngestionAck]
  ) extends Command

  /**
   * Periodic timer to sweep and re-dispatch unacknowledged Transactional Outbox entries (Roadmap 9.3).
   * Extends `NotInfluenceReceiveTimeout` so internal timer ticks do not block idle passivation.
   */
  case object RetryPendingOutbox extends Command with NotInfluenceReceiveTimeout

  /**
   * Periodic timer to trigger a cyclic LLM situation summary if new transactions arrived since the last summary.
   * Extends `NotInfluenceReceiveTimeout` so internal timer ticks do not block idle passivation.
   */
  case object TriggerCyclicLlmSummary extends Command with NotInfluenceReceiveTimeout

  /** Initiates graceful cluster sharding passivation (`shard ! ClusterSharding.Passivate(context.self)`). */
  case object RequestPassivate extends Command

  /** Stop message delivered by the Pekko Shard once messages for this passivating entity are buffered. */
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

  final case class LlmSituationSummarized(
      summary: LlmSituationSummary
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
      pluginRegistry: EventPluginRegistry = EventPluginRegistry.default,
      outboxSweepInterval: FiniteDuration = 5.seconds,
      llmSummarizer: Option[CustomerSituationLlmSummarizer] = None,
      llmTxCountThreshold: Int = 25,
      llmSummaryInterval: FiniteDuration = 15.seconds,
      passivationIdleTimeout: FiniteDuration = 30.minutes,
      shardOpt: Option[ActorRef[ClusterSharding.ShardCommand]] = None
  ): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val llmInFlight = new AtomicBoolean(false)

        context.setReceiveTimeout(passivationIdleTimeout, RequestPassivate)
        timers.startTimerWithFixedDelay("outbox-retry-sweeper", RetryPendingOutbox, outboxSweepInterval)
        if llmSummarizer.isDefined then
          timers.startTimerWithFixedDelay("llm-cyclic-summary-timer", TriggerCyclicLlmSummary, llmSummaryInterval)

        EventSourcedBehavior[Command, Event, CustomerSituationState](
          persistenceId = PersistenceId(TypeKey.name, customerId),
          emptyState = CustomerSituationState.empty(customerId),
          commandHandler = (state, command) =>
            handleCommand(
              customerId = customerId,
              state = state,
              command = command,
              creditEngine = creditEngine,
              peerRiskRouter = peerRiskRouter,
              pluginRegistry = pluginRegistry,
              llmSummarizer = llmSummarizer,
              llmTxCountThreshold = llmTxCountThreshold,
              llmSummaryInterval = llmSummaryInterval,
              llmInFlight = llmInFlight,
              shardOpt = shardOpt,
              context = context,
              timers = timers,
              log = context.log
            ),
          eventHandler = (state, event) => handleEvent(state, event, pluginRegistry)
        ).withRetention(
          RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2)
        ).receiveSignal {
          case (state, RecoveryCompleted) =>
            peerRiskRouter.onAgentWokenUp(customerId, "SHARD_RECOVERY_WAKEUP", state.stateVersion)
            if state.pendingOutbox.nonEmpty then
              context.log.info(
                "[CustomerAgent:{}] RecoveryCompleted with {} pending outbox entries; triggering reliable re-dispatch.",
                customerId,
                state.pendingOutbox.size
              )
              peerRiskRouter.retryPendingOutbox(customerId, state.pendingOutbox.values)
        }
      }
    }

  private def handleCommand(
      customerId: String,
      state: CustomerSituationState,
      command: Command,
      creditEngine: ExplainableCreditEngine,
      peerRiskRouter: PeerRiskRouter,
      pluginRegistry: EventPluginRegistry,
      llmSummarizer: Option[CustomerSituationLlmSummarizer],
      llmTxCountThreshold: Int,
      llmSummaryInterval: FiniteDuration,
      llmInFlight: AtomicBoolean,
      shardOpt: Option[ActorRef[ClusterSharding.ShardCommand]],
      context: ActorContext[Command],
      timers: TimerScheduler[Command],
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
        processObservedEvent(
          customerId = customerId,
          state = state,
          observed = observed,
          replyToOpt = Some(replyTo),
          peerRiskRouter = peerRiskRouter,
          pluginRegistry = pluginRegistry,
          llmSummarizer = llmSummarizer,
          llmTxCountThreshold = llmTxCountThreshold,
          llmInFlight = llmInFlight,
          context = context,
          log = log
        )

      case IngestDocumentFact(fact, replyTo) =>
        val observed = DocumentVerificationAgent.classifyToObservedEvent(fact)
        processObservedEvent(
          customerId = customerId,
          state = state,
          observed = observed,
          replyToOpt = Some(replyTo),
          peerRiskRouter = peerRiskRouter,
          pluginRegistry = pluginRegistry,
          llmSummarizer = llmSummarizer,
          llmTxCountThreshold = llmTxCountThreshold,
          llmInFlight = llmInFlight,
          context = context,
          log = log
        )

      case IngestObservedEvent(observed, replyTo) =>
        processObservedEvent(
          customerId = customerId,
          state = state,
          observed = observed,
          replyToOpt = Some(replyTo),
          peerRiskRouter = peerRiskRouter,
          pluginRegistry = pluginRegistry,
          llmSummarizer = llmSummarizer,
          llmTxCountThreshold = llmTxCountThreshold,
          llmInFlight = llmInFlight,
          context = context,
          log = log
        )

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
          processObservedEvent(
            customerId = customerId,
            state = state,
            observed = observed,
            replyToOpt = replyToOpt,
            peerRiskRouter = peerRiskRouter,
            pluginRegistry = pluginRegistry,
            llmSummarizer = llmSummarizer,
            llmTxCountThreshold = llmTxCountThreshold,
            llmInFlight = llmInFlight,
            context = context,
            log = log
          )

      case IngestNetworkRiskPattern(pattern, replyToOpt) =>
        val observed = GlobalRiskIntelligenceReceived(
          eventId = pattern.patternId,
          customerId = customerId,
          pattern = pattern,
          occurredAt = pattern.detectedAt
        )
        processObservedEvent(
          customerId = customerId,
          state = state,
          observed = observed,
          replyToOpt = replyToOpt,
          peerRiskRouter = peerRiskRouter,
          pluginRegistry = pluginRegistry,
          llmSummarizer = llmSummarizer,
          llmTxCountThreshold = llmTxCountThreshold,
          llmInFlight = llmInFlight,
          context = context,
          log = log
        )

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
        peerRiskRouter.onAgentWokenUp(customerId, "REST_SITUATION_QUERY", state.stateVersion)
        Effect.reply(replyTo)(state)

      case GetOrGenerateSituationDescription(forceRefresh, wakeUpSource, replyTo) =>
        peerRiskRouter.onAgentWokenUp(customerId, wakeUpSource, state.stateVersion)
        val needsLlmRefresh =
          forceRefresh ||
            state.lastLlmSummary.isEmpty ||
            state.txCountSinceLastLlmSummary > 0 ||
            state.lastLlmSummary.exists(_.stateVersion < state.stateVersion)

        llmSummarizer match
          case Some(summarizer) if needsLlmRefresh =>
            llmInFlight.set(true)
            val reason = s"REST_WAKEUP_QUERY($wakeUpSource)"
            context.pipeToSelf(summarizer.summarizeCustomerSituation(state, reason)) {
              case Success(summary) => RecordLlmSituationSummary(summary, Some(replyTo))
              case Failure(ex)      => LlmSummaryFailed(ex.getMessage, state, Some(replyTo))
            }
            Effect.none
          case _ =>
            Effect.reply(replyTo)(state)

      case RecordLlmSituationSummary(summary, replyToOpt) =>
        llmInFlight.set(false)
        Effect
          .persist(LlmSituationSummarized(summary))
          .thenRun { updatedState =>
            peerRiskRouter.onLlmSummaryCompleted(customerId, summary, updatedState)
            replyToOpt.foreach(_ ! updatedState)
          }

      case LlmSummaryFailed(_, fallbackState, replyToOpt) =>
        llmInFlight.set(false)
        replyToOpt.foreach(_ ! fallbackState)
        Effect.none

      case TriggerCyclicLlmSummary =>
        // Only run cyclic LLM summary when the customer has processed new transactions since the last summary and no LLM call is already in flight
        if state.txCountSinceLastLlmSummary > 0 && llmInFlight.compareAndSet(false, true) then
          llmSummarizer match
            case Some(summarizer) =>
              val reason = s"CYCLIC_TIMER(${llmSummaryInterval.toSeconds}s, deltaTx=${state.txCountSinceLastLlmSummary})"
              log.debug("[CustomerAgent:{}] Triggering cyclic LLM situation summary ({})", customerId, reason)
              context.pipeToSelf(summarizer.summarizeCustomerSituation(state, reason)) {
                case Success(summary) => RecordLlmSituationSummary(summary, None)
                case Failure(ex)      => LlmSummaryFailed(ex.getMessage, state, None)
              }
            case None =>
              llmInFlight.set(false)
        Effect.none

      case ResetState(replyTo) =>
        llmInFlight.set(false)
        Effect
          .persist(CustomerStateReset(customerId))
          .thenReply(replyTo)(_ => IngestionAck(customerId, "STATE-RESET"))

      case RetryPendingOutbox =>
        if state.pendingOutbox.nonEmpty then
          log.debug("[CustomerAgent:{}] Sweeping {} unacknowledged outbox entries.", customerId, state.pendingOutbox.size)
          peerRiskRouter.retryPendingOutbox(customerId, state.pendingOutbox.values)
        Effect.none

      case RequestPassivate =>
        log.info("[CustomerAgent:{}] Initiating passivation at stateVersion=v{}; suspending cyclic LLM summaries.", customerId, state.stateVersion)
        timers.cancelAll()
        peerRiskRouter.onAgentPassivated(customerId, state.stateVersion)
        shardOpt match
          case Some(shard) =>
            shard ! ClusterSharding.Passivate(context.self)
            Effect.none
          case None =>
            Effect.stop()

      case Passivate =>
        timers.cancelAll()
        peerRiskRouter.onAgentPassivated(customerId, state.stateVersion)
        Effect.stop()

  private def processObservedEvent(
      customerId: String,
      state: CustomerSituationState,
      observed: AgentObservedEvent,
      replyToOpt: Option[ActorRef[IngestionAck]],
      peerRiskRouter: PeerRiskRouter,
      pluginRegistry: EventPluginRegistry,
      llmSummarizer: Option[CustomerSituationLlmSummarizer],
      llmTxCountThreshold: Int,
      llmInFlight: AtomicBoolean,
      context: ActorContext[Command],
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

          // Trigger cyclic LLM summary when transaction count threshold is reached (guarded by llmInFlight so burst traffic does not flood HTTP pool)
          if llmTxCountThreshold > 0 &&
             updatedState.txCountSinceLastLlmSummary >= llmTxCountThreshold &&
             llmInFlight.compareAndSet(false, true)
          then
            llmSummarizer match
              case Some(summarizer) =>
                val reason = s"CYCLIC_TX_COUNT_THRESHOLD(count=${updatedState.txCountSinceLastLlmSummary}>=$llmTxCountThreshold)"
                context.pipeToSelf(summarizer.summarizeCustomerSituation(updatedState, reason)) {
                  case Success(summary) => RecordLlmSituationSummary(summary, None)
                  case Failure(ex)      => LlmSummaryFailed(ex.getMessage, updatedState, None)
                }
              case None =>
                llmInFlight.set(false)

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
      case LlmSituationSummarized(summary) =>
        state.recordLlmSituationSummary(summary)
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
