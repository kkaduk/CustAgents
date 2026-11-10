package com.bank.sa.api

import com.bank.sa.agent.CustomerClusterGateway
import com.bank.sa.domain.*

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * Typed gRPC Service contract matching `loan_origination.proto` (`com.bank.sa.proto.v1.LoanOriginationService`).
 */
trait LoanOriginationGrpcService:
  def projectLoan(request: LoanOriginationRequest): Future[SituationProjectionDto]
  def evaluateLoan(request: LoanOriginationRequest): Future[LoanOriginationDecisionDto]
  def getCustomerSituation(customerId: String, referenceTime: Instant = Instant.now()): Future[CustomerSituationSnapshotDto]
  def ingestDocumentFact(customerId: String, request: DocumentFactUploadRequest): Future[IngestionAckDto]

/**
 * Default Pekko Cluster Sharding backed implementation of `LoanOriginationGrpcService`.
 * Dispatches unary RPC calls directly to the target `CustomerAgent` and `DocumentVerificationAgent` shard entities.
 */
class LoanOriginationGrpcServiceImpl(
    gateway: CustomerClusterGateway
)(using ec: ExecutionContext)
    extends LoanOriginationGrpcService:

  override def projectLoan(request: LoanOriginationRequest): Future[SituationProjectionDto] =
    val domainApp = request.toDomain
    gateway.projectLoanSituation(domainApp).map(SituationProjectionDto.fromDomain)

  override def evaluateLoan(request: LoanOriginationRequest): Future[LoanOriginationDecisionDto] =
    val domainApp = request.toDomain
    gateway.evaluateLoan(domainApp).map { decision =>
      LoanOriginationDecisionDto.fromDomain(decision, domainApp.estimatedMonthlyInstallment)
    }

  override def getCustomerSituation(
      customerId: String,
      referenceTime: Instant = Instant.now()
  ): Future[CustomerSituationSnapshotDto] =
    gateway.getSituationState(customerId).map { state =>
      CustomerSituationSnapshotDto.fromDomain(state, referenceTime)
    }

  override def ingestDocumentFact(
      customerId: String,
      request: DocumentFactUploadRequest
  ): Future[IngestionAckDto] =
    val fact = request.toDomain(customerId)
    gateway.ingestDocumentFact(fact).map { ack =>
      IngestionAckDto(customerId = ack.customerId, referenceId = ack.referenceId)
    }
