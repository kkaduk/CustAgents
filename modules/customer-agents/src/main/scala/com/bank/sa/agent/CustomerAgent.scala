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
 * a `PeerRiskSignal` to a connected peer's `CustomerAgent` in the Pekko Cluster.
 */
trait PeerRiskRouter extends Serializable:
  def sendRiskSignal(targetCustomerId: String, signal: PeerRiskSignal): Unit

object PeerRiskRouter:
  val NoOp: PeerRiskRouter = (_: String, _: PeerRiskSignal) => ()

/**
 * Digital Twin Actor (`CustomerAgent`) representing a single bank customer.
 * Identified uniquely by `CustomerId` via Pekko Cluster Sharding.
 *
 * Responsibilities:
 * 1. Maintains real-time incremental sliding-window cashflow aggregates in memory.
 * 2. Stores verified external document facts (payslips, tax returns, external debt).
 * 3. Tracks localized Ego-Network connections and propagates 2nd-hop risk signals to peers.
 * 4. Evaluates consumer loan applications in <5ms with full regulatory explainability
 *    and persists an immutable audit snapshot of the decision and underlying situation state.
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

  final case class PropagatePeerRisk(
      signal: PeerRiskSignal,
      replyTo: Option[ActorRef[IngestionAck]] = None
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

  // --- EVENT JOURNAL PROTOCOL ---
  sealed trait Event extends CborSerializable

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
      peerRiskRouter: PeerRiskRouter = PeerRiskRouter.NoOp
  ): Behavior[Command] =
    Behaviors.setup { context =>
      EventSourcedBehavior[Command, Event, CustomerSituationState](
        persistenceId = PersistenceId(TypeKey.name, customerId),
        emptyState = CustomerSituationState.empty(customerId),
        commandHandler = (state, command) =>
          handleCommand(customerId, state, command, creditEngine, peerRiskRouter, context.log),
        eventHandler = (state, event) => handleEvent(state, event)
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
      log: org.slf4j.Logger
  ): Effect[Event, CustomerSituationState] =
    command match
      case IngestTransaction(tx, replyTo) =>
        if state.hasProcessedTx(tx.txId) then
          Effect.reply(replyTo)(IngestionAck(customerId, tx.txId))
        else
          Effect
            .persist[Event, CustomerSituationState](TransactionIngested(tx))
            .thenRun { (updatedState: CustomerSituationState) =>
              propagateRiskIfApplicable(customerId, tx, updatedState, peerRiskRouter, log)
            }
            .thenReply(replyTo)(_ => IngestionAck(customerId, tx.txId))

      case IngestDocumentFact(fact, replyTo) =>
        Effect
          .persist(DocumentFactIngested(fact))
          .thenReply(replyTo)(_ => IngestionAck(customerId, fact.docId))

      case PropagatePeerRisk(signal, replyToOpt) =>
        if signal.hopCount > 2 || signal.originCustomerId == customerId then
          replyToOpt.foreach(_ ! IngestionAck(customerId, signal.signalId))
          Effect.none
        else
          Effect
            .persist(PeerRiskSignalIngested(signal))
            .thenRun { _ =>
              log.info(
                "[CustomerAgent:{}] Recorded {}-hop peer risk signal '{}' from peer '{}' (score={})",
                customerId,
                signal.hopCount,
                signal.signalId,
                signal.fromCustomerId,
                f"${signal.attenuatedRiskScore}%.2f"
              )
              replyToOpt.foreach(_ ! IngestionAck(customerId, signal.signalId))
            }

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

  private def handleEvent(
      state: CustomerSituationState,
      event: Event
  ): CustomerSituationState =
    event match
      case TransactionIngested(tx) =>
        state.recordTransaction(tx)
      case DocumentFactIngested(fact) =>
        state.recordDocumentFact(fact)
      case PeerRiskSignalIngested(signal) =>
        state.recordPeerRiskSignal(signal)
      case CustomerStateReset(cid) =>
        CustomerSituationState.empty(cid)
      case _: LoanDecisionAudited =>
        // Decision audit event is persisted for regulatory replay/immutability; state does not mutate.
        state

  /**
   * Decentralized Graph Risk Propagation:
   * - Scenario A: This transaction is a direct 1st-hop hit with a restricted third party (`tx.restrictedTag.isDefined`).
   *   We notify all currently connected bank peers in `updatedState.connectedBankPeers`.
   * - Scenario B: This transaction is with another bank client (`tx.counterpartyIsBankClient`), and this customer
   *   already carries a 1st-hop restricted exposure. We notify that newly/actively connected bank client.
   */
  private def propagateRiskIfApplicable(
      customerId: String,
      tx: EnrichedTransaction,
      updatedState: CustomerSituationState,
      peerRiskRouter: PeerRiskRouter,
      log: org.slf4j.Logger
  ): Unit =
    tx.restrictedTag match
      case Some(tag) =>
        val attenuatedScore =
          BigDecimal(tag.level.baseRiskWeight * SecondHopAttenuationFactor)
            .setScale(4, BigDecimal.RoundingMode.HALF_UP)
            .toDouble

        updatedState.connectedBankPeers
          .filterNot(_.counterpartyId == customerId)
          .foreach { peerConn =>
            val signal = PeerRiskSignal(
              signalId = s"SIG-${tx.txId}-TO-${peerConn.counterpartyId}",
              originCustomerId = tag.entityId,
              fromCustomerId = customerId,
              targetCustomerId = peerConn.counterpartyId,
              hopCount = 2,
              attenuatedRiskScore = attenuatedScore,
              reasonCode = s"RC_302_PEER_LINK_${tag.level.toString.toUpperCase}",
              reasonDescription =
                s"Peer '$customerId' transacted (${tx.txId}) with restricted entity '${tag.entityId}' [${tag.listSource}: ${tag.reasonDescription}]",
              rootEvidenceIds = (tx.txId :: peerConn.recentTxIds).distinct,
              emittedAt = tx.timestamp
            )
            log.info(
              "[CustomerAgent:{}] Propagating 2nd-hop risk signal '{}' to peer '{}'",
              customerId,
              signal.signalId,
              peerConn.counterpartyId
            )
            peerRiskRouter.sendRiskSignal(peerConn.counterpartyId, signal)
          }

      case None if tx.counterpartyIsBankClient && tx.counterpartyId != customerId =>
        // Check if this customer already has a 1st-hop restricted exposure to propagate to the counterparty
        val directExposures = updatedState.riskExposures.valuesIterator.filter(_.hopDistance == 1).toList
        directExposures.foreach { exposure =>
          val attenuatedScore =
            BigDecimal(exposure.riskScore * SecondHopAttenuationFactor)
              .setScale(4, BigDecimal.RoundingMode.HALF_UP)
              .toDouble

          val signal = PeerRiskSignal(
            signalId = s"SIG-${tx.txId}-FROM-$customerId-TO-${tx.counterpartyId}",
            originCustomerId = exposure.sourceEntityId,
            fromCustomerId = customerId,
            targetCustomerId = tx.counterpartyId,
            hopCount = 2,
            attenuatedRiskScore = attenuatedScore,
            reasonCode = exposure.reasonCode.replace("1HOP", "2HOP"),
            reasonDescription =
              s"Peer '$customerId' has prior restricted exposure (${exposure.description}) and transacted (${tx.txId}) with '${tx.counterpartyId}'",
            rootEvidenceIds = (tx.txId :: exposure.evidenceIds).distinct,
            emittedAt = tx.timestamp
          )
          log.info(
            "[CustomerAgent:{}] Propagating existing 1st-hop exposure via 2nd-hop signal '{}' to peer '{}'",
            customerId,
            signal.signalId,
            tx.counterpartyId
          )
          peerRiskRouter.sendRiskSignal(tx.counterpartyId, signal)
        }

      case _ => ()
