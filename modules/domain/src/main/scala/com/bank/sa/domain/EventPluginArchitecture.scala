package com.bank.sa.domain

import java.time.Instant

/**
 * Snapshot record emitted when `CustomerAgent` updates its situational awareness after processing an event
 * (`Event 1006: SituationEvaluated` in Section 2.1 of Doc/ExaPekdoDesign.md).
 */
final case class SituationEvaluatedRecord(
    customerId: String,
    triggerEventId: String,
    triggerEventType: String,
    handledByPluginId: String,
    stateVersion: Long,
    ruleVersion: String,
    summary: CustomerSituationSummary,
    completeness: DataCompleteness,
    evaluatedAt: Instant
) extends Serializable

/**
 * Result produced by a `SituationEventPlugin` after processing an `AgentObservedEvent`:
 * - `updatedState`: new persistent context of the agent (including any Transactional Outbox entries)
 * - `outboxEntries`: newly generated side-effect intents for Reliable Delivery
 * - `situationEvaluated`: updated situational awareness record (`SituationEvaluated`)
 */
final case class PluginProcessingResult(
    updatedState: CustomerSituationState,
    outboxEntries: List[OutboxEntry],
    situationEvaluated: SituationEvaluatedRecord
) extends Serializable

/**
 * Known Plugin Interface for processing observed events in `CustomerAgent` (Section 2.1):
 * "Każde zdarzenie obserwowane przez agenta powinno mieć własną logikę przetwarzania, tak aby można było je
 *  konfigurować poprzez architekturę plugin implementujący znany interfejs przetwarzania zdarzeń."
 */
trait SituationEventPlugin extends Serializable:
  def pluginId: String
  def supportedEventType: String
  def canHandle(event: AgentObservedEvent): Boolean = event.eventType == supportedEventType
  def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult

/**
 * Plugin 1: Processes `TransactionObserved` (Event 1001)
 * Updates sliding-window cashflow buckets, Ego-Network connections, 1st-hop restricted exposure,
 * and creates Transactional Outbox entries for 2nd-hop peer risk propagation and RelationshipAgent updates.
 */
class TransactionObservedPlugin(secondHopAttenuation: Double = 0.65) extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.transaction-observed.v1"
  override val supportedEventType: String = "TransactionObserved"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case TransactionObserved(eventId, customerId, tx, occurredAt) =>
        val stateAfterTx = state.recordTransaction(tx)
        val outbox       = buildOutboxEntries(customerId, tx, stateAfterTx, occurredAt)
        val finalState   = stateAfterTx.enqueueOutbox(outbox)
        PluginProcessingResult(
          updatedState = finalState,
          outboxEntries = outbox,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = finalState.stateVersion,
            ruleVersion = finalState.ruleVersion,
            summary = finalState.situationSummary,
            completeness = finalState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

  private def buildOutboxEntries(
      customerId: String,
      tx: EnrichedTransaction,
      updatedState: CustomerSituationState,
      now: Instant
  ): List[OutboxEntry] =
    val relId = List(customerId, tx.counterpartyId).sorted.mkString("<->")
    val relOutbox = OutboxEntry(
      outboxId = s"OUTBOX-REL-${tx.txId}-$customerId",
      sourceCustomerId = customerId,
      targetType = OutboxTargetType.RelationshipAgent,
      targetEntityId = relId,
      messageType = "RelationshipInteractionRecorded",
      payloadSummary = s"Tx ${tx.txId} (${tx.direction} ${tx.amount} ${tx.currency}) between $customerId and ${tx.counterpartyId}",
      peerSignal = None,
      createdAt = now
    )

    val riskOutboxEntries: List[OutboxEntry] = tx.restrictedTag match
      case Some(tag) =>
        val attenuatedScore =
          BigDecimal(tag.level.baseRiskWeight * secondHopAttenuation)
            .setScale(4, BigDecimal.RoundingMode.HALF_UP)
            .toDouble

        updatedState.connectedBankPeers
          .filterNot(_.counterpartyId == customerId)
          .map { peerConn =>
            val outboxId = s"OUTBOX-SIG-${tx.txId}-TO-${peerConn.counterpartyId}"
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
              emittedAt = tx.timestamp,
              outboxId = Some(outboxId)
            )
            OutboxEntry(
              outboxId = outboxId,
              sourceCustomerId = customerId,
              targetType = OutboxTargetType.RiskSignalAgent,
              targetEntityId = peerConn.counterpartyId,
              messageType = "PropagatePeerRisk",
              payloadSummary = s"2nd-hop risk signal ${signal.signalId} to ${peerConn.counterpartyId} (score=$attenuatedScore)",
              peerSignal = Some(signal),
              createdAt = now
            )
          }

      case None if tx.counterpartyIsBankClient && tx.counterpartyId != customerId =>
        val directExposures = updatedState.riskExposures.valuesIterator.filter(_.hopDistance == 1).toList
        directExposures.map { exposure =>
          val attenuatedScore =
            BigDecimal(exposure.riskScore * secondHopAttenuation)
              .setScale(4, BigDecimal.RoundingMode.HALF_UP)
              .toDouble
          val outboxId = s"OUTBOX-SIG-${tx.txId}-FROM-$customerId-TO-${tx.counterpartyId}"
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
            emittedAt = tx.timestamp,
            outboxId = Some(outboxId)
          )
          OutboxEntry(
            outboxId = outboxId,
            sourceCustomerId = customerId,
            targetType = OutboxTargetType.RiskSignalAgent,
            targetEntityId = tx.counterpartyId,
            messageType = "PropagatePeerRisk",
            payloadSummary = s"2nd-hop risk signal ${signal.signalId} to ${tx.counterpartyId} (score=$attenuatedScore)",
            peerSignal = Some(signal),
            createdAt = now
          )
        }

      case _ => Nil

    relOutbox :: riskOutboxEntries

/**
 * Plugin 2: Processes `IncomeVerified` (Event 1002)
 */
class IncomeVerifiedPlugin extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.income-verified.v1"
  override val supportedEventType: String = "IncomeVerified"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case IncomeVerified(eventId, customerId, fact, _, occurredAt) =>
        val updatedState = state.recordDocumentFact(fact)
        PluginProcessingResult(
          updatedState = updatedState,
          outboxEntries = Nil,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = updatedState.stateVersion,
            ruleVersion = updatedState.ruleVersion,
            summary = updatedState.situationSummary,
            completeness = updatedState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

/**
 * Plugin 3: Processes `DebtUpdated` (Event 1003)
 */
class DebtUpdatedPlugin extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.debt-updated.v1"
  override val supportedEventType: String = "DebtUpdated"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case DebtUpdated(eventId, customerId, fact, _, _, occurredAt) =>
        val updatedState = state.recordDocumentFact(fact)
        PluginProcessingResult(
          updatedState = updatedState,
          outboxEntries = Nil,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = updatedState.stateVersion,
            ruleVersion = updatedState.ruleVersion,
            summary = updatedState.situationSummary,
            completeness = updatedState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

/**
 * Plugin 4: Processes `CounterpartyRiskReceived` (Event 1004)
 */
class CounterpartyRiskReceivedPlugin extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.counterparty-risk-received.v1"
  override val supportedEventType: String = "CounterpartyRiskReceived"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case CounterpartyRiskReceived(eventId, customerId, signal, occurredAt) =>
        val updatedState = state.recordPeerRiskSignal(signal)
        PluginProcessingResult(
          updatedState = updatedState,
          outboxEntries = Nil,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = updatedState.stateVersion,
            ruleVersion = updatedState.ruleVersion,
            summary = updatedState.situationSummary,
            completeness = updatedState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

/**
 * Plugin 5: Processes `ExternalDocumentReceived` (Event 1005)
 */
class ExternalDocumentReceivedPlugin extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.external-document-received.v1"
  override val supportedEventType: String = "ExternalDocumentReceived"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case ExternalDocumentReceived(eventId, customerId, fact, occurredAt) =>
        val updatedState = state.recordDocumentFact(fact)
        PluginProcessingResult(
          updatedState = updatedState,
          outboxEntries = Nil,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = updatedState.stateVersion,
            ruleVersion = updatedState.ruleVersion,
            summary = updatedState.situationSummary,
            completeness = updatedState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

/**
 * Plugin 6: Processes `GlobalRiskIntelligenceReceived` / `NetworkRiskPatternDetected` from Oracle Exadata Global Graph (Section 5 & 14)
 */
class GlobalRiskIntelligencePlugin extends SituationEventPlugin:
  override val pluginId: String           = "plugin.sa.global-risk-intelligence.v1"
  override val supportedEventType: String = "NetworkRiskPatternDetected"

  override def process(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    event match
      case GlobalRiskIntelligenceReceived(eventId, customerId, pattern, occurredAt) =>
        val updatedState = state.recordNetworkRiskPattern(pattern)
        PluginProcessingResult(
          updatedState = updatedState,
          outboxEntries = Nil,
          situationEvaluated = SituationEvaluatedRecord(
            customerId = customerId,
            triggerEventId = eventId,
            triggerEventType = supportedEventType,
            handledByPluginId = pluginId,
            stateVersion = updatedState.stateVersion,
            ruleVersion = updatedState.ruleVersion,
            summary = updatedState.situationSummary,
            completeness = updatedState.dataCompleteness,
            evaluatedAt = occurredAt
          )
        )
      case other =>
        throw new IllegalArgumentException(s"$pluginId cannot process ${other.eventType}")

/**
 * Configurable Registry routing incoming `AgentObservedEvent`s to the appropriate `SituationEventPlugin` (Section 2.1).
 */
class EventPluginRegistry(
    val plugins: List[SituationEventPlugin] = EventPluginRegistry.defaultPlugins
) extends Serializable:

  private val byEventType: Map[String, SituationEventPlugin] =
    plugins.map(p => p.supportedEventType -> p).toMap

  def registeredPluginIds: List[String] = plugins.map(_.pluginId)

  def pluginFor(event: AgentObservedEvent): Option[SituationEventPlugin] =
    byEventType.get(event.eventType).orElse(plugins.find(_.canHandle(event)))

  def routeAndProcess(state: CustomerSituationState, event: AgentObservedEvent): PluginProcessingResult =
    pluginFor(event) match
      case Some(plugin) =>
        plugin.process(state, event)
      case None =>
        throw new IllegalStateException(
          s"No registered SituationEventPlugin found for eventType='${event.eventType}' (registered: ${byEventType.keys.mkString(", ")})"
        )

object EventPluginRegistry:
  def defaultPlugins: List[SituationEventPlugin] = List(
    new TransactionObservedPlugin(),
    new IncomeVerifiedPlugin(),
    new DebtUpdatedPlugin(),
    new CounterpartyRiskReceivedPlugin(),
    new ExternalDocumentReceivedPlugin(),
    new GlobalRiskIntelligencePlugin()
  )

  val default: EventPluginRegistry = new EventPluginRegistry(defaultPlugins)

