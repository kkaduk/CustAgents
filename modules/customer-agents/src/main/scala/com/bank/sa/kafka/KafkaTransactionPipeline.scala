package com.bank.sa.kafka

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.api.JacksonJsonSupport
import com.bank.sa.domain.*
import com.bank.sa.enrichment.EnrichmentLogic
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.net.{InetSocketAddress, Socket}
import java.time.Instant
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.util.control.NonFatal

/**
 * Configuration request for generating high-volume transaction streams (including Hot-Partition customers) into Kafka.
 */
final case class KafkaGeneratorRequest(
    totalTransactions: Int = 2000,
    normalCustomersCount: Int = 50,
    hotPartitionCustomerIds: List[String] = List("CUST-HOT-MEGA-01", "CUST-HOT-WHALE-02", "CUST-ALICE-001"),
    hotPartitionTrafficShare: Double = 0.65,
    highRiskRatio: Double = 0.08,
    topic: String = "sa.transactions.raw.v1"
) extends Serializable

/**
 * Summary returned immediately after publishing a generated transaction batch to Kafka.
 */
final case class KafkaGenerationBatchResult(
    batchId: String,
    topic: String,
    brokerMode: String,
    generatedTransactionsCount: Int,
    hotPartitionTransactionsCount: Int,
    normalTransactionsCount: Int,
    partitionCounts: Map[Int, Int],
    hotCustomerCounts: Map[String, Int],
    generationTimeMs: Long,
    publishedAt: Instant
) extends Serializable

/**
 * Real-time telemetry of the Kafka Transaction Pipeline (Producer + Partition Skew + Automatic Consumer).
 */
final case class KafkaPipelineStatusDto(
    brokerMode: String,
    bootstrapServers: String,
    topic: String,
    numPartitions: Int,
    consumerRunning: Boolean,
    totalProduced: Long,
    totalConsumedAndEnriched: Long,
    totalRoutedToAgents: Long,
    pendingQueueLag: Long,
    lastBatchGeneratedCount: Int,
    lastBatchDurationMs: Long,
    partitionDistribution: Map[Int, Long],
    hotCustomersProducedCounts: Map[String, Long]
) extends Serializable

/**
 * End-to-End Kafka Transaction Generator & Automatic Consumer Pipeline:
 * 1. Generates thousands of realistic banking transactions, including configurable Hot-Partition customers
 *    (e.g., 65% of traffic concentrated on 2-3 hot keys landing on specific Kafka partitions).
 * 2. Publishes raw transactions to Kafka (`sa.transactions.raw.v1`) — using `org.apache.kafka.clients.producer.KafkaProducer`
 *    when a live broker at `KAFKA_BOOTSTRAP_SERVERS` (`localhost:9092`) is reachable, and maintaining an in-process
 *    16-partitioned topic queue so automatic consumption works seamlessly in all environments.
 * 3. Automatically consumes transactions in the background, enriches them via Flink `EnrichmentLogic`,
 *    and routes them to sharded `CustomerAgent` actors (waking them up if passivated and triggering cyclic LLM summaries).
 */
class KafkaTransactionPipeline(
    gateway: CustomerClusterGateway,
    watchlistProvider: () => Map[String, RestrictedEntityTag],
    val bootstrapServers: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    val defaultTopic: String = "sa.transactions.raw.v1",
    val numPartitions: Int = 16
)(using ec: ExecutionContext):

  private val log = LoggerFactory.getLogger(getClass)

  private val totalProducedCounter      = new AtomicLong(0L)
  private val totalConsumedCounter      = new AtomicLong(0L)
  private val totalRoutedCounter        = new AtomicLong(0L)
  private val lastBatchCount            = new AtomicInteger(0)
  private val lastBatchDurationMs       = new AtomicLong(0L)
  private val txSequence                = new AtomicLong(100000L)
  private val consumerActive            = new AtomicBoolean(false)
  private val drainInProgress           = new AtomicBoolean(false)

  // 16 explicit Kafka partitions (0 .. numPartitions-1)
  private val partitionedQueues: Vector[ConcurrentLinkedQueue[RawTransaction]] =
    Vector.fill(numPartitions)(new ConcurrentLinkedQueue[RawTransaction]())

  private val partitionProducedCounts: Vector[AtomicLong] =
    Vector.fill(numPartitions)(new AtomicLong(0L))

  private val hotCustomerCounts = new ConcurrentHashMap[String, AtomicLong]()

  @volatile private var liveKafkaProducerOpt: Option[KafkaProducer[String, String]] = None
  @volatile private var brokerModeLabel: String = "EMBEDDED_PARTITIONED_KAFKA"

  private val consumerScheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r =>
      val t = new Thread(r, "sa-kafka-auto-consumer")
      t.setDaemon(true)
      t
    }

  initializeKafkaProducerIfReachable()
  startAutomaticConsumer()

  private def isBrokerReachable(servers: String): Boolean =
    if sys.env.get("KAFKA_DISABLE_LIVE_PROBE").contains("true") then false
    else
      try
        val first = servers.split(",").head.trim
        val parts = first.split(":")
        val host  = parts(0)
        val port  = if parts.length > 1 then parts(1).toInt else 9092
        val sock  = new Socket()
        try
          sock.connect(new InetSocketAddress(host, port), 250)
          true
        finally sock.close()
      catch case NonFatal(_) => false

  private def initializeKafkaProducerIfReachable(): Unit =
    if isBrokerReachable(bootstrapServers) then
      try
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.ACKS_CONFIG, "1")
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
        liveKafkaProducerOpt = Some(new KafkaProducer[String, String](props))
        brokerModeLabel = s"LIVE_KAFKA_BROKER($bootstrapServers)"
        log.info("Connected KafkaProducer to live Kafka broker at {}", bootstrapServers)
      catch
        case NonFatal(ex) =>
          log.warn("Could not initialize live KafkaProducer ({}); using embedded partitioned Kafka topic.", ex.getMessage)
          liveKafkaProducerOpt = None
          brokerModeLabel = "EMBEDDED_PARTITIONED_KAFKA"

  /**
   * Deterministic Kafka partition assignment by `customerId` key.
   * Ensures all transactions for a hot customer hit the exact same partition (Hot-Partition effect).
   */
  def partitionForCustomer(customerId: String): Int =
    math.abs(CustomerSituationState.digest64(customerId).toInt % numPartitions)

  /**
   * Generates a batch of `totalTransactions` (including hot-partition customers) and publishes them to Kafka.
   * The background consumer automatically picks them up, enriches them via Flink logic, and routes them to `CustomerAgent`s.
   */
  def generateAndPublishTransactions(request: KafkaGeneratorRequest): KafkaGenerationBatchResult =
    val startNanos = System.nanoTime()
    val now        = Instant.now()
    val totalCount = math.max(1, math.min(50000, request.totalTransactions))
    val normalPool = (1 to math.max(1, request.normalCustomersCount)).map(i => f"CUST-NORM-$i%04d").toVector
    val hotPool =
      if request.hotPartitionCustomerIds.nonEmpty then request.hotPartitionCustomerIds.toVector
      else Vector("CUST-HOT-MEGA-01", "CUST-HOT-WHALE-02", "CUST-ALICE-001")
    val hotShare = math.max(0.0, math.min(1.0, request.hotPartitionTrafficShare))
    val hrRatio  = math.max(0.0, math.min(1.0, request.highRiskRatio))

    val rng       = new Random()
    var hotCount  = 0
    var normCount = 0
    val batchPartitionCounts = scala.collection.mutable.Map.empty[Int, Int].withDefaultValue(0)
    val batchHotCounts       = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)

    val counterpartiesNormal = Vector(
      ("METRO-GROCERY-MKT", false, TransactionCategory.EverydaySpend, "GROCERY SUPERMARKET"),
      ("CITY-ENERGY-UTIL", false, TransactionCategory.Utilities, "ELECTRICITY & WATER BILL"),
      ("PRIME-LANDLORD-RE", false, TransactionCategory.Housing, "MONTHLY APARTMENT RENT"),
      ("GLOBAL-TECH-PAYROLL", false, TransactionCategory.Salary, "MONTHLY SALARY PAYROLL")
    )

    val counterpartiesHighRisk = Vector(
      ("CASINO-ROYALE-BET", false, TransactionCategory.Gambling, "CASINO ROYALE BETTING TERMINAL"),
      ("BINANCE-CRYPTO-BRIDGE", false, TransactionCategory.CryptoExchange, "CRYPTO EXCHANGE TRANSFER"),
      ("ENTITY-SHADOW-LTD", false, TransactionCategory.HighRiskTransfer, "OFFSHORE SHADOW ENTITY"),
      ("UNLICENSED-LENDER-88", false, TransactionCategory.HighRiskTransfer, "UNLICENSED LENDER PAYOUT")
    )

    var idx = 0
    while idx < totalCount do
      val seqId    = txSequence.incrementAndGet()
      val isHot    = rng.nextDouble() < hotShare
      val isRisky  = rng.nextDouble() < hrRatio
      val txTime   = now.minusMillis((totalCount - idx).toLong * 5L)

      val customerId =
        if isHot then
          val chosenHot = hotPool(rng.nextInt(hotPool.size))
          hotCount += 1
          batchHotCounts(chosenHot) = batchHotCounts(chosenHot) + 1
          hotCustomerCounts.computeIfAbsent(chosenHot, _ => new AtomicLong(0L)).incrementAndGet()
          chosenHot
        else
          normCount += 1
          normalPool(rng.nextInt(normalPool.size))

      val (counterpartyId, cpIsBankClient, category, desc, amount, overdraft) =
        if isRisky then
          val (cp, isClient, cat, d) = counterpartiesHighRisk(rng.nextInt(counterpartiesHighRisk.size))
          val amt = BigDecimal(200 + rng.nextInt(2500))
          (cp, isClient, cat, d, amt, rng.nextDouble() < 0.20)
        else if rng.nextDouble() < 0.25 then
          // P2P transfer between bank clients
          val peer = normalPool(rng.nextInt(normalPool.size))
          val targetPeer = if peer == customerId then "CUST-BOB-002" else peer
          (targetPeer, true, TransactionCategory.PeerTransfer, "P2P INSTANT TRANSFER", BigDecimal(50 + rng.nextInt(600)), false)
        else
          val (cp, isClient, cat, d) = counterpartiesNormal(rng.nextInt(counterpartiesNormal.size))
          val amt = if cat == TransactionCategory.Salary then BigDecimal(3500 + rng.nextInt(3000)) else BigDecimal(30 + rng.nextInt(450))
          (cp, isClient, cat, d, amt, false)

      val rawTx = RawTransaction(
        txId = s"KFK-TX-$seqId",
        fromCustomerId = customerId,
        toCounterpartyId = counterpartyId,
        counterpartyIsBankClient = cpIsBankClient,
        amount = amount,
        currency = "PLN",
        category = category,
        resultsInOverdraft = overdraft,
        timestamp = txTime,
        merchantDescription = Some(desc),
        fromAccountId = Some(s"PL10-${customerId.takeRight(8)}-ACC1"),
        toAccountId = Some(s"PL90-${counterpartyId.takeRight(8)}-ACC1")
      )

      val part = publishSingleRawTransaction(request.topic, rawTx)
      batchPartitionCounts(part) = batchPartitionCounts(part) + 1
      idx += 1

    val elapsedMs = math.max(1L, (System.nanoTime() - startNanos) / 1_000_000L)
    lastBatchCount.set(totalCount)
    lastBatchDurationMs.set(elapsedMs)

    val batchId = s"KAFKA-BATCH-${now.toEpochMilli}"
    gateway.recordTrace(
      stage = "KAFKA_INGESTION",
      fromComponent = "KafkaTransactionGenerator",
      toComponent = s"KafkaTopic(${request.topic}, partitions=$numPartitions)",
      eventType = s"GeneratedBatch ($totalCount txs, HotShare=${(hotShare * 100).toInt}%)",
      summary =
        s"Published $totalCount transactions ($hotCount hot-partition txs across [${hotPool.mkString(", ")}], $normCount normal txs) to '${request.topic}' in ${elapsedMs}ms",
      details = Map(
        "batchId" -> batchId,
        "totalTransactions" -> totalCount.toString,
        "hotPartitionTransactions" -> hotCount.toString,
        "normalTransactions" -> normCount.toString,
        "hotCustomers" -> batchHotCounts.map { case (k, v) => s"$k:$v" }.mkString(", "),
        "brokerMode" -> brokerModeLabel,
        "generationTimeMs" -> s"${elapsedMs}ms"
      ),
      timestamp = now
    )

    // Trigger immediate asynchronous drain tick so processing starts right away
    triggerAsyncDrain()

    KafkaGenerationBatchResult(
      batchId = batchId,
      topic = request.topic,
      brokerMode = brokerModeLabel,
      generatedTransactionsCount = totalCount,
      hotPartitionTransactionsCount = hotCount,
      normalTransactionsCount = normCount,
      partitionCounts = batchPartitionCounts.toMap,
      hotCustomerCounts = batchHotCounts.toMap,
      generationTimeMs = elapsedMs,
      publishedAt = now
    )

  /**
   * Publishes a single `RawTransaction` to the assigned Kafka partition (and live `KafkaProducer` if connected).
   */
  def publishSingleRawTransaction(topic: String, rawTx: RawTransaction): Int =
    val partition = partitionForCustomer(rawTx.fromCustomerId)
    partitionedQueues(partition).offer(rawTx)
    partitionProducedCounts(partition).incrementAndGet()
    totalProducedCounter.incrementAndGet()

    liveKafkaProducerOpt.foreach { producer =>
      try
        val json = JacksonJsonSupport.mapper.writeValueAsString(rawTx)
        val rec  = new ProducerRecord[String, String](topic, Integer.valueOf(partition), rawTx.fromCustomerId, json)
        producer.send(rec)
      catch
        case NonFatal(ex) =>
          log.debug("KafkaProducer async send fallback: {}", ex.getMessage)
    }
    partition

  /**
   * Starts the automatic background Kafka consumer that polls partitions, enriches via Flink logic,
   * and routes transactions to sharded `CustomerAgent` actors.
   */
  def startAutomaticConsumer(): Unit =
    if consumerActive.compareAndSet(false, true) then
      consumerScheduler.scheduleWithFixedDelay(
        () => triggerAsyncDrain(),
        50L,
        100L,
        TimeUnit.MILLISECONDS
      )

  def triggerAsyncDrain(): Unit =
    if drainInProgress.compareAndSet(false, true) then
      Future {
        try
          drainAvailableTransactions(maxRecordsPerTick = 500)
        finally
          drainInProgress.set(false)
          if pendingQueueLag > 0 then triggerAsyncDrain()
      }

  /**
   * Drains up to `maxRecordsPerTick` raw transactions round-robin across all 16 partitions,
   * enriches them via `EnrichmentLogic`, and dispatches them to `CustomerClusterGateway.ingestTransaction`.
   */
  def drainAvailableTransactions(maxRecordsPerTick: Int = 500): Future[Int] =
    val batch = scala.collection.mutable.ListBuffer.empty[RawTransaction]
    var p     = 0
    while batch.size < maxRecordsPerTick && p < numPartitions do
      val q = partitionedQueues(p)
      var polledFromPartition = 0
      while batch.size < maxRecordsPerTick && polledFromPartition < 64 && !q.isEmpty do
        val item = q.poll()
        if item != null then
          batch += item
          polledFromPartition += 1
      p += 1

    if batch.isEmpty then Future.successful(0)
    else
      val rawList      = batch.toList
      val watchlist    = watchlistProvider()
      val enrichedList = EnrichmentLogic.enrichBatchInMemory(rawList, watchlist)
      totalConsumedCounter.addAndGet(rawList.size.toLong)

      // Process in bounded chunks of 64 to keep actor mailbox responsiveness high even under 10,000+ tx bursts
      val chunks = enrichedList.grouped(64).toList
      chunks
        .foldLeft(Future.successful(0)) { (accFut, chunk) =>
          accFut.flatMap { countSoFar =>
            Future.traverse(chunk)(tx => gateway.ingestTransaction(tx)).map { acks =>
              totalRoutedCounter.addAndGet(acks.size.toLong)
              countSoFar + acks.size
            }
          }
        }

  def pendingQueueLag: Long =
    partitionedQueues.map(_.size().toLong).sum

  def getStatus: KafkaPipelineStatusDto =
    val partMap = (0 until numPartitions).map(i => i -> partitionProducedCounts(i).get()).toMap
    val hotMap  = hotCustomerCounts.asScala.map { case (k, v) => k -> v.get() }.toMap
    KafkaPipelineStatusDto(
      brokerMode = brokerModeLabel,
      bootstrapServers = bootstrapServers,
      topic = defaultTopic,
      numPartitions = numPartitions,
      consumerRunning = consumerActive.get(),
      totalProduced = totalProducedCounter.get(),
      totalConsumedAndEnriched = totalConsumedCounter.get(),
      totalRoutedToAgents = totalRoutedCounter.get(),
      pendingQueueLag = pendingQueueLag,
      lastBatchGeneratedCount = lastBatchCount.get(),
      lastBatchDurationMs = lastBatchDurationMs.get(),
      partitionDistribution = partMap,
      hotCustomersProducedCounts = hotMap
    )

  def resetCounters(): Unit =
    partitionedQueues.foreach(_.clear())
    partitionProducedCounts.foreach(_.set(0L))
    hotCustomerCounts.clear()
    totalProducedCounter.set(0L)
    totalConsumedCounter.set(0L)
    totalRoutedCounter.set(0L)
    lastBatchCount.set(0)
    lastBatchDurationMs.set(0L)

  def close(): Unit =
    consumerActive.set(false)
    consumerScheduler.shutdownNow()
    liveKafkaProducerOpt.foreach(p => scala.util.Try(p.close()))

