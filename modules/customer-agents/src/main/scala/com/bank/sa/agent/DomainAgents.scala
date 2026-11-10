package com.bank.sa.agent

import com.bank.sa.domain.*
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}

import java.time.Instant

// =============================================================================
// 1. RelationshipAgent — Stan określonej relacji lub ekspozycji (Section 6 & 15)
// =============================================================================
object RelationshipAgent:

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("RelationshipAgent")

  sealed trait Command extends CborSerializable

  final case class RecordInteraction(
      tx: EnrichedTransaction,
      riskScore: Double,
      replyTo: Option[ActorRef[RelationshipSituationState]] = None
  ) extends Command

  final case class RecordPropagatedRisk(
      signalScore: Double,
      emittedAt: Instant,
      replyTo: Option[ActorRef[RelationshipSituationState]] = None
  ) extends Command

  final case class GetRelationshipState(
      replyTo: ActorRef[RelationshipSituationState]
  ) extends Command

  final case class ResetRelationship(
      replyTo: ActorRef[CustomerAgent.IngestionAck]
  ) extends Command

  case object Passivate extends Command

  sealed trait Event extends CborSerializable
  final case class InteractionRecorded(tx: EnrichedTransaction, riskScore: Double) extends Event
  final case class PropagatedRiskRecorded(signalScore: Double, emittedAt: Instant) extends Event
  final case class RelationshipReset(relationshipId: String) extends Event

  def parseParties(relationshipId: String): (String, String) =
    relationshipId.split("<->", 2) match
      case Array(a, b) => (a, b)
      case _           => (relationshipId, "UNKNOWN")

  def apply(relationshipId: String): Behavior[Command] =
    val (partyA, partyB) = parseParties(relationshipId)
    Behaviors.setup { _ =>
      EventSourcedBehavior[Command, Event, RelationshipSituationState](
        persistenceId = PersistenceId(TypeKey.name, relationshipId),
        emptyState = RelationshipSituationState(relationshipId = relationshipId, partyA = partyA, partyB = partyB),
        commandHandler = (state, cmd) =>
          cmd match
            case RecordInteraction(tx, riskScore, replyToOpt) =>
              Effect
                .persist(InteractionRecorded(tx, riskScore))
                .thenRun { updated => replyToOpt.foreach(_ ! updated) }
            case RecordPropagatedRisk(score, at, replyToOpt) =>
              Effect
                .persist(PropagatedRiskRecorded(score, at))
                .thenRun { updated => replyToOpt.foreach(_ ! updated) }
            case GetRelationshipState(replyTo) =>
              Effect.reply(replyTo)(state)
            case ResetRelationship(replyTo) =>
              Effect
                .persist(RelationshipReset(relationshipId))
                .thenReply(replyTo)(_ => CustomerAgent.IngestionAck(relationshipId, "REL-RESET"))
            case Passivate =>
              Effect.stop(),
        eventHandler = (state, evt) =>
          evt match
            case InteractionRecorded(tx, riskScore) =>
              state.recordInteraction(tx, riskScore)
            case PropagatedRiskRecorded(score, at) =>
              state.recordRiskPropagation(score, at)
            case RelationshipReset(relId) =>
              val (a, b) = parseParties(relId)
              RelationshipSituationState(relationshipId = relId, partyA = a, partyB = b)
      ).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2))
    }

// =============================================================================
// 2. RiskSignalAgent — Interpretacja i cykl życia sygnałów ryzyka (Section 6 & 10)
// =============================================================================
object RiskSignalAgent:

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("RiskSignalAgent")

  sealed trait Command extends CborSerializable

  final case class DispatchAndTrackSignal(
      signal: PeerRiskSignal,
      replyTo: Option[ActorRef[RiskSignalAgentState]] = None
  ) extends Command

  final case class AcknowledgeSignalDelivery(
      signalId: String,
      acknowledgedAt: Instant,
      replyTo: Option[ActorRef[RiskSignalAgentState]] = None
  ) extends Command

  final case class GetRiskSignalState(
      replyTo: ActorRef[RiskSignalAgentState]
  ) extends Command

  final case class ResetRiskSignals(
      replyTo: ActorRef[CustomerAgent.IngestionAck]
  ) extends Command

  case object Passivate extends Command

  sealed trait Event extends CborSerializable
  final case class SignalRegistered(signal: PeerRiskSignal) extends Event
  final case class SignalAcknowledged(signalId: String, acknowledgedAt: Instant) extends Event
  final case class RiskSignalsReset(agentKey: String) extends Event

  def apply(
      agentKey: String,
      deliverToCustomer: (String, PeerRiskSignal) => Unit = (_, _) => ()
  ): Behavior[Command] =
    Behaviors.setup { _ =>
      EventSourcedBehavior[Command, Event, RiskSignalAgentState](
        persistenceId = PersistenceId(TypeKey.name, agentKey),
        emptyState = RiskSignalAgentState(agentKey = agentKey),
        commandHandler = (state, cmd) =>
          cmd match
            case DispatchAndTrackSignal(signal, replyToOpt) =>
              if state.hasSeenSignal(signal.signalId) then
                replyToOpt.foreach(_ ! state)
                Effect.none
              else
                Effect
                  .persist(SignalRegistered(signal))
                  .thenRun { updated =>
                    deliverToCustomer(signal.targetCustomerId, signal)
                    replyToOpt.foreach(_ ! updated)
                  }

            case AcknowledgeSignalDelivery(signalId, ackTime, replyToOpt) =>
              Effect
                .persist(SignalAcknowledged(signalId, ackTime))
                .thenRun { updated => replyToOpt.foreach(_ ! updated) }

            case GetRiskSignalState(replyTo) =>
              Effect.reply(replyTo)(state)

            case ResetRiskSignals(replyTo) =>
              Effect
                .persist(RiskSignalsReset(agentKey))
                .thenReply(replyTo)(_ => CustomerAgent.IngestionAck(agentKey, "RISK-SIGNALS-RESET"))

            case Passivate =>
              Effect.stop(),
        eventHandler = (state, evt) =>
          evt match
            case SignalRegistered(signal) =>
              state.registerSignal(signal)
            case SignalAcknowledged(signalId, ackTime) =>
              state.markDelivered(signalId, ackTime)
            case RiskSignalsReset(key) =>
              RiskSignalAgentState(agentKey = key)
      ).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2))
    }

// =============================================================================
// 3. DocumentVerificationAgent — Koordynacja weryfikacji dokumentów (Section 6)
// =============================================================================
object DocumentVerificationAgent:

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("DocumentVerificationAgent")

  sealed trait Command extends CborSerializable

  final case class VerifyAndClassifyDocument(
      fact: VerifiedDocumentFact,
      replyTo: ActorRef[AgentObservedEvent]
  ) extends Command

  final case class GetVerificationState(
      replyTo: ActorRef[DocumentVerificationAgentState]
  ) extends Command

  final case class ResetVerificationState(
      replyTo: ActorRef[CustomerAgent.IngestionAck]
  ) extends Command

  case object Passivate extends Command

  sealed trait Event extends CborSerializable
  final case class DocumentVerifiedEvent(fact: VerifiedDocumentFact, derivedEventType: String) extends Event
  final case class VerificationReset(customerId: String) extends Event

  /**
   * Translates a verified document fact into the appropriate observed domain event for `CustomerAgent`
   * (`IncomeVerified` Event 1002, `DebtUpdated` Event 1003, or `ExternalDocumentReceived` Event 1005).
   */
  def classifyToObservedEvent(fact: VerifiedDocumentFact): AgentObservedEvent =
    fact.factType match
      case DocumentFactType.VerifiedMonthlyNetIncome =>
        IncomeVerified(
          eventId = fact.docId,
          customerId = fact.customerId,
          fact = fact,
          monthlyNetIncome = fact.numericValue.getOrElse(BigDecimal(0)),
          occurredAt = fact.verifiedAt
        )
      case DocumentFactType.TaxReturnAnnualIncome =>
        val monthlyEquivalent = fact.numericValue.map(v => (v / BigDecimal(12)).setScale(2, BigDecimal.RoundingMode.HALF_UP)).getOrElse(BigDecimal(0))
        IncomeVerified(
          eventId = fact.docId,
          customerId = fact.customerId,
          fact = fact,
          monthlyNetIncome = monthlyEquivalent,
          occurredAt = fact.verifiedAt
        )
      case DocumentFactType.ExternalDebtObligation =>
        DebtUpdated(
          eventId = fact.docId,
          customerId = fact.customerId,
          fact = fact,
          monthlyDebtAmount = fact.numericValue.getOrElse(BigDecimal(0)),
          creditor = fact.issuer,
          occurredAt = fact.verifiedAt
        )
      case _ =>
        ExternalDocumentReceived(
          eventId = fact.docId,
          customerId = fact.customerId,
          fact = fact,
          occurredAt = fact.verifiedAt
        )

  def apply(customerId: String): Behavior[Command] =
    Behaviors.setup { _ =>
      EventSourcedBehavior[Command, Event, DocumentVerificationAgentState](
        persistenceId = PersistenceId(TypeKey.name, customerId),
        emptyState = DocumentVerificationAgentState(customerId = customerId),
        commandHandler = (state, cmd) =>
          cmd match
            case VerifyAndClassifyDocument(fact, replyTo) =>
              val observed = classifyToObservedEvent(fact)
              Effect
                .persist(DocumentVerifiedEvent(fact, observed.eventType))
                .thenReply(replyTo)(_ => observed)

            case GetVerificationState(replyTo) =>
              Effect.reply(replyTo)(state)

            case ResetVerificationState(replyTo) =>
              Effect
                .persist(VerificationReset(customerId))
                .thenReply(replyTo)(_ => CustomerAgent.IngestionAck(customerId, "DOC-VERIFICATION-RESET"))

            case Passivate =>
              Effect.stop(),
        eventHandler = (state, evt) =>
          evt match
            case DocumentVerifiedEvent(fact, derivedType) =>
              state.recordVerifiedDocument(fact, derivedType)
            case VerificationReset(cid) =>
              DocumentVerificationAgentState(customerId = cid)
      ).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2))
    }

// =============================================================================
// 4. PortfolioRiskAgent — Interpretacja ryzyka na poziomie segmentu lub portfela (Section 6 & 15)
// =============================================================================
object PortfolioRiskAgent:

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("PortfolioRiskAgent")

  sealed trait Command extends CborSerializable

  final case class UpdateCustomerInPortfolio(
      customerState: CustomerSituationState,
      replyTo: Option[ActorRef[PortfolioSituationState]] = None
  ) extends Command

  final case class IngestGlobalRiskPattern(
      pattern: NetworkRiskPatternDetected,
      replyTo: Option[ActorRef[PortfolioSituationState]] = None
  ) extends Command

  final case class GetPortfolioSituation(
      replyTo: ActorRef[PortfolioSituationState]
  ) extends Command

  final case class ResetPortfolio(
      replyTo: ActorRef[CustomerAgent.IngestionAck]
  ) extends Command

  case object Passivate extends Command

  sealed trait Event extends CborSerializable
  final case class CustomerPortfolioSnapshotUpdated(customerState: CustomerSituationState) extends Event
  final case class GlobalPatternAddedToPortfolio(pattern: NetworkRiskPatternDetected) extends Event
  final case class PortfolioReset(segmentId: String) extends Event

  def apply(
      segmentId: String,
      onSnapshotUpdated: PortfolioSituationState => Unit = _ => ()
  ): Behavior[Command] =
    Behaviors.setup { _ =>
      EventSourcedBehavior[Command, Event, PortfolioSituationState](
        persistenceId = PersistenceId(TypeKey.name, segmentId),
        emptyState = PortfolioSituationState(segmentId = segmentId),
        commandHandler = (state, cmd) =>
          cmd match
            case UpdateCustomerInPortfolio(customerState, replyToOpt) =>
              Effect
                .persist(CustomerPortfolioSnapshotUpdated(customerState))
                .thenRun { updated =>
                  onSnapshotUpdated(updated)
                  replyToOpt.foreach(_ ! updated)
                }

            case IngestGlobalRiskPattern(pattern, replyToOpt) =>
              Effect
                .persist(GlobalPatternAddedToPortfolio(pattern))
                .thenRun { updated =>
                  onSnapshotUpdated(updated)
                  replyToOpt.foreach(_ ! updated)
                }

            case GetPortfolioSituation(replyTo) =>
              Effect.reply(replyTo)(state)

            case ResetPortfolio(replyTo) =>
              Effect
                .persist(PortfolioReset(segmentId))
                .thenReply(replyTo)(_ => CustomerAgent.IngestionAck(segmentId, "PORTFOLIO-RESET"))

            case Passivate =>
              Effect.stop(),
        eventHandler = (state, evt) =>
          evt match
            case CustomerPortfolioSnapshotUpdated(customerState) =>
              state.updateCustomerEntry(customerState)
            case GlobalPatternAddedToPortfolio(pattern) =>
              state.recordGlobalPattern(pattern)
            case PortfolioReset(segId) =>
              PortfolioSituationState(segmentId = segId)
      ).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 50, keepNSnapshots = 2))
    }

