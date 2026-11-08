package com.bank.sa.agent

import com.bank.sa.domain.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

/**
 * Reactive Pekko Streams ingestion pipeline connecting enriched event streams
 * (`customer.situation.events.v1` and `doc.verified.v1`) to the Pekko Cluster Sharding region.
 *
 * Uses `groupBy` on `customerId` (substreams aligned with shards) + `mapAsync(1)` per customer
 * to guarantee strict per-customer sequential ordering while allowing high concurrency across distinct customers.
 */
class SituationStreamProcessor(
    gateway: CustomerClusterGateway,
    maxConcurrentSubstreams: Int = 256
)(using system: ActorSystem[_]):

  /**
   * Backpressured Pekko Stream `Flow` that routes `EnrichedTransaction` events to their
   * respective `CustomerAgent` shard entities and emits `IngestionAck` upon journal persistence.
   */
  def transactionIngestionFlow: Flow[EnrichedTransaction, CustomerAgent.IngestionAck, NotUsed] =
    Flow[EnrichedTransaction]
      .groupBy(maxConcurrentSubstreams, tx => math.abs(tx.customerId.hashCode % maxConcurrentSubstreams))
      .mapAsync(1)(tx => gateway.ingestTransaction(tx))
      .mergeSubstreams

  /**
   * Backpressured Pekko Stream `Flow` that routes `VerifiedDocumentFact` events to their
   * respective `CustomerAgent` shard entities.
   */
  def documentFactIngestionFlow: Flow[VerifiedDocumentFact, CustomerAgent.IngestionAck, NotUsed] =
    Flow[VerifiedDocumentFact]
      .groupBy(maxConcurrentSubstreams, doc => math.abs(doc.customerId.hashCode % maxConcurrentSubstreams))
      .mapAsync(1)(doc => gateway.ingestDocumentFact(doc))
      .mergeSubstreams

  def ingestTransactionStream(
      source: Source[EnrichedTransaction, NotUsed]
  ): Future[Seq[CustomerAgent.IngestionAck]] =
    source.via(transactionIngestionFlow).runWith(Sink.seq)

  def ingestDocumentStream(
      source: Source[VerifiedDocumentFact, NotUsed]
  ): Future[Seq[CustomerAgent.IngestionAck]] =
    source.via(documentFactIngestionFlow).runWith(Sink.seq)

