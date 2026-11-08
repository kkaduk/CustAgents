package com.bank.sa.domain

import java.time.{Duration, Instant}

/** Pre-computed or instant-evaluated metrics for a specific time window (e.g. 24h, 7d, 30d). */
final case class WindowMetrics(
    windowDuration: Duration,
    totalInflow: BigDecimal,
    totalOutflow: BigDecimal,
    highRiskSpend: BigDecimal,
    overdraftCount: Int,
    inflowTxIds: List[String],
    outflowTxIds: List[String],
    highRiskTxIds: List[String],
    overdraftTxIds: List[String]
) extends Serializable:
  def netCashflow: BigDecimal = totalInflow - totalOutflow

  def highRiskToInflowRatio: BigDecimal =
    if totalInflow > BigDecimal(0) then
      (highRiskSpend / totalInflow).setScale(4, BigDecimal.RoundingMode.HALF_UP)
    else if highRiskSpend > BigDecimal(0) then BigDecimal(1.0)
    else BigDecimal(0.0)

/** Hourly bucket of aggregated transaction activity for O(1) incremental updates. */
final case class HourlyCashflowBucket(
    epochHour: Long,
    inflow: BigDecimal = BigDecimal(0),
    outflow: BigDecimal = BigDecimal(0),
    highRiskSpend: BigDecimal = BigDecimal(0),
    overdraftCount: Int = 0,
    inflowTxIds: List[String] = Nil,
    outflowTxIds: List[String] = Nil,
    highRiskTxIds: List[String] = Nil,
    overdraftTxIds: List[String] = Nil
) extends Serializable:

  def record(tx: EnrichedTransaction): HourlyCashflowBucket =
    val (newIn, newOut, newInIds, newOutIds) = tx.direction match
      case TransactionDirection.Inflow =>
        (inflow + tx.amount, outflow, (tx.txId :: inflowTxIds).take(25), outflowTxIds)
      case TransactionDirection.Outflow =>
        (inflow, outflow + tx.amount, inflowTxIds, (tx.txId :: outflowTxIds).take(25))

    val isHighRisk = tx.direction == TransactionDirection.Outflow &&
      (tx.category.isHighRiskCategory || tx.restrictedTag.isDefined)

    val (newHrSpend, newHrIds) =
      if isHighRisk then (highRiskSpend + tx.amount, (tx.txId :: highRiskTxIds).take(25))
      else (highRiskSpend, highRiskTxIds)

    val (newOdCount, newOdIds) =
      if tx.resultsInOverdraft then (overdraftCount + 1, (tx.txId :: overdraftTxIds).take(25))
      else (overdraftCount, overdraftTxIds)

    copy(
      inflow = newIn,
      outflow = newOut,
      highRiskSpend = newHrSpend,
      overdraftCount = newOdCount,
      inflowTxIds = newInIds,
      outflowTxIds = newOutIds,
      highRiskTxIds = newHrIds,
      overdraftTxIds = newOdIds
    )

/**
 * Incremental time-bucketed ring buffer maintained in memory inside each `CustomerAgent`.
 * Eliminates expensive SQL GROUP BY / window queries over billions of transaction rows.
 */
final case class SlidingWindowCashflow(
    buckets: Map[Long, HourlyCashflowBucket] = Map.empty,
    retentionHours: Long = 24L * 90L // 90 days of hourly buckets max
) extends Serializable:

  private def toEpochHour(instant: Instant): Long =
    instant.getEpochSecond / 3600L

  def record(tx: EnrichedTransaction): SlidingWindowCashflow =
    val txHour       = toEpochHour(tx.timestamp)
    val updatedEntry = buckets.getOrElse(txHour, HourlyCashflowBucket(txHour)).record(tx)
    val maxHour      = math.max(txHour, buckets.keysIterator.maxOption.getOrElse(txHour))
    val cutoffHour   = maxHour - retentionHours
    val pruned       = (buckets + (txHour -> updatedEntry)).filter { case (h, _) => h >= cutoffHour }
    copy(buckets = pruned)

  def metricsForWindow(referenceTime: Instant, window: Duration): WindowMetrics =
    val endHour   = toEpochHour(referenceTime)
    val startHour = toEpochHour(referenceTime.minus(window))
    val relevant  = buckets.valuesIterator.filter(b => b.epochHour > startHour && b.epochHour <= endHour).toList

    relevant.foldLeft(
      WindowMetrics(
        windowDuration = window,
        totalInflow = BigDecimal(0),
        totalOutflow = BigDecimal(0),
        highRiskSpend = BigDecimal(0),
        overdraftCount = 0,
        inflowTxIds = Nil,
        outflowTxIds = Nil,
        highRiskTxIds = Nil,
        overdraftTxIds = Nil
      )
    ) { (acc, b) =>
      acc.copy(
        totalInflow = acc.totalInflow + b.inflow,
        totalOutflow = acc.totalOutflow + b.outflow,
        highRiskSpend = acc.highRiskSpend + b.highRiskSpend,
        overdraftCount = acc.overdraftCount + b.overdraftCount,
        inflowTxIds = (acc.inflowTxIds ++ b.inflowTxIds).distinct.take(30),
        outflowTxIds = (acc.outflowTxIds ++ b.outflowTxIds).distinct.take(30),
        highRiskTxIds = (acc.highRiskTxIds ++ b.highRiskTxIds).distinct.take(30),
        overdraftTxIds = (acc.overdraftTxIds ++ b.overdraftTxIds).distinct.take(30)
      )
    }

