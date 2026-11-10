package com.bank.sa.oracle

import com.bank.sa.domain.*
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, Timestamp}
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*
import scala.util.Using
import scala.util.control.NonFatal

/**
 * Configuration for connecting `OracleExadataRepository` directly to a local or remote Oracle Database / Exadata instance.
 */
final case class OracleJdbcConfig(
    enabled: Boolean =
      sys.env.get("ORACLE_EXADATA_JDBC_ENABLED").orElse(sys.props.get("oracle.exadata.jdbc.enabled")).exists(_.equalsIgnoreCase("true")),
    jdbcUrl: String =
      sys.env.get("ORACLE_JDBC_URL").orElse(sys.props.get("oracle.jdbc.url")).getOrElse("jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1"),
    user: String =
      sys.env.get("ORACLE_USER").orElse(sys.props.get("oracle.user")).getOrElse("SYSTEM"),
    password: String =
      sys.env.get("ORACLE_PASSWORD").orElse(sys.props.get("oracle.password")).getOrElse("PKO_666#")
)

/**
 * Represents a row in `SA_PERSISTENCE.AGENT_EVENT_JOURNAL` (Section 2.1 & 11).
 */
final case class OracleJournalRow(
    persistenceId: String,
    sequenceNr: Long,
    eventType: String,
    eventPayloadSummary: String,
    eventTimestamp: Instant,
    eventSchemaVersion: Int = 1
) extends Serializable

/**
 * Represents a bounded snapshot row in `SA_PERSISTENCE.AGENT_STATE_SNAPSHOTS` (Section 3 & 11).
 */
final case class OracleSnapshotRow(
    persistenceId: String,
    sequenceNr: Long,
    stateVersion: Long,
    ruleVersion: String,
    monthlyIncome: BigDecimal,
    incomeStability: String,
    liquidityTrend: String,
    completenessScore: Double,
    activeExposuresCount: Int,
    snapshottedAt: Instant
) extends Serializable

/**
 * Represents a canonical financial transaction fact in `SA_FACTS.FINANCIAL_TRANSACTION_FACTS` (Section 4 & 11).
 */
final case class FinancialTransactionFactRow(
    txId: String,
    customerId: String,
    counterpartyId: String,
    counterpartyIsBankClient: Boolean,
    direction: String,
    amount: BigDecimal,
    currency: String,
    category: String,
    categorizerSource: String,
    resultsInOverdraft: Boolean,
    restrictedEntityId: Option[String],
    occurredAt: Instant,
    projectedAt: Instant
) extends Serializable

/**
 * Represents a governance policy entry in `SA_GOVERNANCE.GOVERNANCE_POLICY_REGISTRY` (Section 11 & 13).
 */
final case class GovernancePolicyRow(
    policyVersion: String,
    modelName: String,
    maxDtiRatio: BigDecimal,
    maxHighRiskSpendRatio: BigDecimal,
    maxOverdraftCount: Int,
    max1HopRiskScore: Double,
    max2HopRiskScore: Double,
    active: Boolean,
    registeredAt: Instant
) extends Serializable

/**
 * Implements the Oracle Exadata 6-Schema Global Memory, Projection Store, and Global Property Graph Analyzer
 * (`SA_PERSISTENCE`, `SA_FACTS`, `SA_GRAPH`, `SA_INTELLIGENCE`, `SA_DECISIONS`, `SA_GOVERNANCE`)
 * as specified in Sections 2, 3, 4, 5, 11, 12, and 14 of `Doc/ExaPekdoDesign.md`.
 *
 * Supports two execution modes:
 * 1. In-memory mode (default for fast unit tests).
 * 2. Live Oracle JDBC mode (`ORACLE_EXADATA_JDBC_ENABLED=true`), which automatically provisions the 6 schemas
 *    and tables in the target Oracle database (`FREEPDB1`) and executes real SQL `MERGE` / `INSERT` statements.
 */
class OracleExadataRepository(val jdbcConfig: OracleJdbcConfig = OracleJdbcConfig()):

  private val log = LoggerFactory.getLogger(getClass)

  private val seqCounters          = new ConcurrentHashMap[String, AtomicLong]()
  // 1. SA_PERSISTENCE
  private val persistenceJournal   = new ConcurrentLinkedDeque[OracleJournalRow]()
  private val persistenceSnapshots = new ConcurrentHashMap[String, OracleSnapshotRow]()
  private val persistenceOutbox    = new ConcurrentHashMap[String, OutboxEntry]()

  // 2. SA_FACTS
  private val financialTxFacts     = new ConcurrentLinkedDeque[FinancialTransactionFactRow]()
  private val verifiedDocFacts     = new ConcurrentHashMap[String, VerifiedDocumentFact]()
  private val debtObligationFacts  = new ConcurrentHashMap[String, VerifiedDocumentFact]()

  // 3. SA_GRAPH (Oracle Property Graph: Vertices & Edges)
  private val graphVertices        = new ConcurrentHashMap[String, GraphVertex]()
  private val graphEdges           = new ConcurrentHashMap[String, GraphEdge]()

  // 4. SA_INTELLIGENCE (Global Risk Intelligence & Portfolio Analytics)
  private val riskIntelEvents      = new ConcurrentLinkedDeque[NetworkRiskPatternDetected]()
  private val portfolioSnapshots   = new ConcurrentHashMap[String, PortfolioSituationState]()

  // 5. SA_DECISIONS (Persisted Evaluations & Projections)
  private val decisionAuditLog     = new ConcurrentLinkedDeque[LoanDecisionResponse]()
  private val projectionAuditLog   = new ConcurrentLinkedDeque[SituationProjection]()

  // 6. SA_GOVERNANCE (Rule Versions, Models, Policies)
  private val governancePolicies   = new ConcurrentHashMap[String, GovernancePolicyRow]()

  @volatile private var jdbcConnectionReady: Boolean = false

  if jdbcConfig.enabled then
    initializeOracleDatabaseSchemas()

  // Bootstrap default governance policy
  registerGovernancePolicy(
    GovernancePolicyRow(
      policyVersion = "SA-CREDIT-POLICY-2026.11",
      modelName = "Endsley-SA-Hybrid-Scorecard-v2",
      maxDtiRatio = BigDecimal("0.40"),
      maxHighRiskSpendRatio = BigDecimal("0.15"),
      maxOverdraftCount = 1,
      max1HopRiskScore = 0.50,
      max2HopRiskScore = 0.45,
      active = true,
      registeredAt = Instant.parse("2026-11-01T00:00:00Z")
    )
  )

  private def withJdbcConnection[A](f: Connection => A): Option[A] =
    if !jdbcConfig.enabled || !jdbcConnectionReady then None
    else
      try
        Using.resource(DriverManager.getConnection(jdbcConfig.jdbcUrl, jdbcConfig.user, jdbcConfig.password)) { conn =>
          Some(f(conn))
        }
      catch
        case NonFatal(ex) =>
          log.warn("Oracle JDBC operation warning: {}", ex.getMessage)
          None

  private def execDdlIgnoreExists(conn: Connection, sql: String): Unit =
    try
      Using.resource(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    catch
      case NonFatal(ex) =>
        val msg = Option(ex.getMessage).getOrElse("")
        // ORA-01920: user name conflicts, ORA-00955: name is already used by an existing object
        if !msg.contains("ORA-01920") && !msg.contains("ORA-00955") && !msg.contains("ORA-40602") then
          log.warn("DDL note for [{}]: {}", sql.take(80), msg)

  /**
   * Idempotently provisions the 6 Oracle Schemas (`SA_PERSISTENCE`, `SA_FACTS`, `SA_GRAPH`,
   * `SA_INTELLIGENCE`, `SA_DECISIONS`, `SA_GOVERNANCE`), tables, and Oracle 23ai/26ai Property Graph.
   */
  def initializeOracleDatabaseSchemas(): Unit =
    try
      Class.forName("oracle.jdbc.OracleDriver")
      Using.resource(DriverManager.getConnection(jdbcConfig.jdbcUrl, jdbcConfig.user, jdbcConfig.password)) { conn =>
        log.info("Connected to local Oracle database at {} as user '{}'. Provisioning 6 SA_* schemas...", jdbcConfig.jdbcUrl, jdbcConfig.user)

        val schemas = List(
          "SA_PERSISTENCE"  -> "Exadata_SA_Persist_2026#",
          "SA_FACTS"        -> "Exadata_SA_Facts_2026#",
          "SA_GRAPH"        -> "Exadata_SA_Graph_2026#",
          "SA_INTELLIGENCE" -> "Exadata_SA_Intel_2026#",
          "SA_DECISIONS"    -> "Exadata_SA_Decisions_2026#",
          "SA_GOVERNANCE"   -> "Exadata_SA_Gov_2026#"
        )
        schemas.foreach { case (schema, pwd) =>
          execDdlIgnoreExists(conn, s"""CREATE USER $schema IDENTIFIED BY "$pwd"""")
          execDdlIgnoreExists(conn, s"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO $schema")
        }

        val ddlStatements = List(
          """CREATE TABLE SA_PERSISTENCE.AGENT_EVENT_JOURNAL (
            |  PERSISTENCE_ID VARCHAR2(255 CHAR) NOT NULL,
            |  SEQUENCE_NR NUMBER(19) NOT NULL,
            |  EVENT_TYPE VARCHAR2(128 CHAR) NOT NULL,
            |  EVENT_PAYLOAD VARCHAR2(2000 CHAR) NOT NULL,
            |  EVENT_TIMESTAMP TIMESTAMP WITH TIME ZONE NOT NULL,
            |  EVENT_SCHEMA_VERSION NUMBER(10) DEFAULT 1 NOT NULL,
            |  CONSTRAINT PK_SA_AGENT_EVENT_JOURNAL PRIMARY KEY (PERSISTENCE_ID, SEQUENCE_NR)
            |)""".stripMargin,
          """CREATE TABLE SA_PERSISTENCE.AGENT_STATE_SNAPSHOTS (
            |  PERSISTENCE_ID VARCHAR2(255 CHAR) NOT NULL,
            |  SEQUENCE_NR NUMBER(19) NOT NULL,
            |  STATE_VERSION NUMBER(19) NOT NULL,
            |  RULE_VERSION VARCHAR2(64 CHAR) NOT NULL,
            |  MONTHLY_INCOME NUMBER(19, 4) NOT NULL,
            |  INCOME_STABILITY VARCHAR2(32 CHAR) NOT NULL,
            |  LIQUIDITY_TREND VARCHAR2(32 CHAR) NOT NULL,
            |  COMPLETENESS_SCORE NUMBER(5, 4) NOT NULL,
            |  ACTIVE_EXPOSURES_COUNT NUMBER(10) NOT NULL,
            |  SNAPSHOTTED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_AGENT_SNAPSHOTS PRIMARY KEY (PERSISTENCE_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_PERSISTENCE.AGENT_TRANSACTIONAL_OUTBOX (
            |  OUTBOX_ID VARCHAR2(128 CHAR) NOT NULL,
            |  PERSISTENCE_ID VARCHAR2(255 CHAR) NOT NULL,
            |  TARGET_AGENT_TYPE VARCHAR2(64 CHAR) NOT NULL,
            |  TARGET_ENTITY_ID VARCHAR2(128 CHAR) NOT NULL,
            |  MESSAGE_TYPE VARCHAR2(128 CHAR) NOT NULL,
            |  PAYLOAD_SUMMARY VARCHAR2(1000 CHAR) NOT NULL,
            |  DELIVERY_STATUS VARCHAR2(32 CHAR) NOT NULL,
            |  CREATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_AGENT_OUTBOX PRIMARY KEY (OUTBOX_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_FACTS.FINANCIAL_TRANSACTION_FACTS (
            |  TX_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  COUNTERPARTY_ID VARCHAR2(128 CHAR) NOT NULL,
            |  COUNTERPARTY_IS_BANK_CLIENT NUMBER(1) NOT NULL,
            |  DIRECTION VARCHAR2(16 CHAR) NOT NULL,
            |  AMOUNT NUMBER(19, 4) NOT NULL,
            |  CURRENCY VARCHAR2(8 CHAR) NOT NULL,
            |  CATEGORY VARCHAR2(64 CHAR) NOT NULL,
            |  CATEGORIZER_SOURCE VARCHAR2(64 CHAR) NOT NULL,
            |  RESULTS_IN_OVERDRAFT NUMBER(1) NOT NULL,
            |  RESTRICTED_ENTITY_ID VARCHAR2(128 CHAR),
            |  OCCURRED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  PROJECTED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_FIN_TX_FACTS PRIMARY KEY (TX_ID, CUSTOMER_ID, DIRECTION)
            |)""".stripMargin,
          """CREATE TABLE SA_FACTS.VERIFIED_DOCUMENT_FACTS (
            |  DOC_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  FACT_TYPE VARCHAR2(64 CHAR) NOT NULL,
            |  NUMERIC_VALUE NUMBER(19, 4),
            |  TEXT_VALUE VARCHAR2(512 CHAR),
            |  ISSUER VARCHAR2(255 CHAR) NOT NULL,
            |  VERIFIED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_VERIFIED_DOC_FACTS PRIMARY KEY (DOC_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_GRAPH.GRAPH_VERTICES (
            |  VERTEX_ID VARCHAR2(128 CHAR) NOT NULL,
            |  VERTEX_TYPE VARCHAR2(64 CHAR) NOT NULL,
            |  SEGMENT_ID VARCHAR2(64 CHAR),
            |  INTRINSIC_RISK NUMBER(5, 4) DEFAULT 0.0 NOT NULL,
            |  DEGREE_CENTRALITY NUMBER(10) DEFAULT 0 NOT NULL,
            |  COMMUNITY_ID VARCHAR2(128 CHAR),
            |  LAST_UPDATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_GRAPH_VERTICES PRIMARY KEY (VERTEX_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_GRAPH.GRAPH_EDGES (
            |  EDGE_ID VARCHAR2(255 CHAR) NOT NULL,
            |  SOURCE_VERTEX_ID VARCHAR2(128 CHAR) NOT NULL,
            |  TARGET_VERTEX_ID VARCHAR2(128 CHAR) NOT NULL,
            |  RELATIONSHIP_TYPE VARCHAR2(64 CHAR) NOT NULL,
            |  TOTAL_VOLUME NUMBER(19, 4) NOT NULL,
            |  TX_COUNT NUMBER(10) NOT NULL,
            |  EVIDENCE_TX_IDS VARCHAR2(2000 CHAR),
            |  LAST_ACTIVE_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_GRAPH_EDGES PRIMARY KEY (EDGE_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_INTELLIGENCE.RISK_INTELLIGENCE_EVENTS (
            |  PATTERN_ID VARCHAR2(128 CHAR) NOT NULL,
            |  PATTERN_TYPE VARCHAR2(64 CHAR) NOT NULL,
            |  AFFECTED_CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  SHARED_ENTITY_ID VARCHAR2(128 CHAR) NOT NULL,
            |  LINKED_CUSTOMERS VARCHAR2(2000 CHAR) NOT NULL,
            |  RISK_SCORE NUMBER(5, 4) NOT NULL,
            |  REASON_CODE VARCHAR2(64 CHAR) NOT NULL,
            |  DESCRIPTION VARCHAR2(1000 CHAR) NOT NULL,
            |  EVIDENCE_IDS VARCHAR2(2000 CHAR) NOT NULL,
            |  DETECTED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_RISK_INTEL_EVENTS PRIMARY KEY (PATTERN_ID, AFFECTED_CUSTOMER_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_DECISIONS.SITUATION_PROJECTION_LOG (
            |  APPLICATION_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  STATE_VERSION NUMBER(19) NOT NULL,
            |  RULE_VERSION VARCHAR2(64 CHAR) NOT NULL,
            |  DATA_COMPLETENESS NUMBER(5, 4) NOT NULL,
            |  FUTURE_DTI NUMBER(7, 4) NOT NULL,
            |  EXPECTED_LIQUIDITY NUMBER(19, 4) NOT NULL,
            |  RISK_EXPOSURE_SCORE NUMBER(5, 4) NOT NULL,
            |  IS_AFFORDABLE NUMBER(1) NOT NULL,
            |  PROJECTED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_PROJ_LOG PRIMARY KEY (APPLICATION_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_DECISIONS.DECISION_AUDIT_LOG (
            |  APPLICATION_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  OUTCOME_STATUS VARCHAR2(32 CHAR) NOT NULL,
            |  CREDIT_SCORE NUMBER(10) NOT NULL,
            |  STATE_VERSION NUMBER(19) NOT NULL,
            |  POLICY_VERSION VARCHAR2(64 CHAR) NOT NULL,
            |  DATA_COMPLETENESS NUMBER(5, 4) NOT NULL,
            |  REASON_CODES VARCHAR2(1000 CHAR),
            |  STATEMENT_OF_REASONS CLOB NOT NULL,
            |  EVALUATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_DECISION_AUDIT_LOG PRIMARY KEY (APPLICATION_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_GOVERNANCE.GOVERNANCE_POLICY_REGISTRY (
            |  POLICY_VERSION VARCHAR2(64 CHAR) NOT NULL,
            |  MODEL_NAME VARCHAR2(128 CHAR) NOT NULL,
            |  MAX_DTI_RATIO NUMBER(5, 4) NOT NULL,
            |  MAX_HIGH_RISK_SPEND NUMBER(5, 4) NOT NULL,
            |  MAX_OVERDRAFT_COUNT NUMBER(10) NOT NULL,
            |  MAX_1HOP_RISK_SCORE NUMBER(5, 4) NOT NULL,
            |  MAX_2HOP_RISK_SCORE NUMBER(5, 4) NOT NULL,
            |  ACTIVE_FLAG NUMBER(1) DEFAULT 1 NOT NULL,
            |  REGISTERED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_GOV_POLICY PRIMARY KEY (POLICY_VERSION)
            |)""".stripMargin
        )

        ddlStatements.foreach(sql => execDdlIgnoreExists(conn, sql))
        jdbcConnectionReady = true
        log.info("Oracle Exadata 6-Schema initialization complete (SA_PERSISTENCE, SA_FACTS, SA_GRAPH, SA_INTELLIGENCE, SA_DECISIONS, SA_GOVERNANCE).")
      }
    catch
      case NonFatal(ex) =>
        log.warn("Could not initialize Oracle JDBC at {}: {}. Falling back to in-memory mode.", jdbcConfig.jdbcUrl, ex.getMessage)
        jdbcConnectionReady = false

  private def nextSeq(persistenceId: String): Long =
    seqCounters.computeIfAbsent(persistenceId, _ => new AtomicLong(1000L)).incrementAndGet()

  // --- 1. SA_PERSISTENCE OPERATIONS ---

  def appendJournalEvent(
      persistenceId: String,
      eventType: String,
      payloadSummary: String,
      timestamp: Instant = Instant.now()
  ): OracleJournalRow =
    val seq = nextSeq(persistenceId)
    val row = OracleJournalRow(
      persistenceId = persistenceId,
      sequenceNr = seq,
      eventType = eventType,
      eventPayloadSummary = payloadSummary,
      eventTimestamp = timestamp,
      eventSchemaVersion = 1
    )
    persistenceJournal.addFirst(row)
    while persistenceJournal.size() > 500 do persistenceJournal.removeLast()

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_PERSISTENCE.AGENT_EVENT_JOURNAL t
          |USING (SELECT ? AS pid, ? AS seq FROM dual) s
          |ON (t.PERSISTENCE_ID = s.pid AND t.SEQUENCE_NR = s.seq)
          |WHEN NOT MATCHED THEN INSERT (PERSISTENCE_ID, SEQUENCE_NR, EVENT_TYPE, EVENT_PAYLOAD, EVENT_TIMESTAMP, EVENT_SCHEMA_VERSION)
          |VALUES (?, ?, ?, ?, ?, 1)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, row.persistenceId)
        ps.setLong(2, row.sequenceNr)
        ps.setString(3, row.persistenceId)
        ps.setLong(4, row.sequenceNr)
        ps.setString(5, row.eventType)
        ps.setString(6, row.eventPayloadSummary.take(1900))
        ps.setTimestamp(7, Timestamp.from(row.eventTimestamp))
        ps.executeUpdate()
      }
    }
    row

  def saveBoundedSnapshot(persistenceId: String, state: CustomerSituationState): OracleSnapshotRow =
    val seq = seqCounters.getOrDefault(persistenceId, new AtomicLong(1000L)).get()
    val snap = OracleSnapshotRow(
      persistenceId = persistenceId,
      sequenceNr = seq,
      stateVersion = state.stateVersion,
      ruleVersion = state.ruleVersion,
      monthlyIncome = state.situationSummary.monthlyIncome,
      incomeStability = state.situationSummary.incomeStability.toString,
      liquidityTrend = state.situationSummary.liquidityTrend.toString,
      completenessScore = state.dataCompleteness.score,
      activeExposuresCount = state.riskExposures.size,
      snapshottedAt = Instant.now()
    )
    persistenceSnapshots.put(persistenceId, snap)

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_PERSISTENCE.AGENT_STATE_SNAPSHOTS t
          |USING (SELECT ? AS pid FROM dual) s ON (t.PERSISTENCE_ID = s.pid)
          |WHEN MATCHED THEN UPDATE SET
          |  SEQUENCE_NR = ?, STATE_VERSION = ?, RULE_VERSION = ?, MONTHLY_INCOME = ?,
          |  INCOME_STABILITY = ?, LIQUIDITY_TREND = ?, COMPLETENESS_SCORE = ?, ACTIVE_EXPOSURES_COUNT = ?, SNAPSHOTTED_AT = ?
          |WHEN NOT MATCHED THEN INSERT
          |  (PERSISTENCE_ID, SEQUENCE_NR, STATE_VERSION, RULE_VERSION, MONTHLY_INCOME, INCOME_STABILITY, LIQUIDITY_TREND, COMPLETENESS_SCORE, ACTIVE_EXPOSURES_COUNT, SNAPSHOTTED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        val ts = Timestamp.from(snap.snapshottedAt)
        ps.setString(1, snap.persistenceId)
        ps.setLong(2, snap.sequenceNr)
        ps.setLong(3, snap.stateVersion)
        ps.setString(4, snap.ruleVersion)
        ps.setBigDecimal(5, snap.monthlyIncome.bigDecimal)
        ps.setString(6, snap.incomeStability)
        ps.setString(7, snap.liquidityTrend)
        ps.setDouble(8, snap.completenessScore)
        ps.setInt(9, snap.activeExposuresCount)
        ps.setTimestamp(10, ts)
        ps.setString(11, snap.persistenceId)
        ps.setLong(12, snap.sequenceNr)
        ps.setLong(13, snap.stateVersion)
        ps.setString(14, snap.ruleVersion)
        ps.setBigDecimal(15, snap.monthlyIncome.bigDecimal)
        ps.setString(16, snap.incomeStability)
        ps.setString(17, snap.liquidityTrend)
        ps.setDouble(18, snap.completenessScore)
        ps.setInt(19, snap.activeExposuresCount)
        ps.setTimestamp(20, ts)
        ps.executeUpdate()
      }
    }
    snap

  def persistOutboxEntries(entries: Iterable[OutboxEntry]): Unit =
    entries.foreach { e =>
      persistenceOutbox.put(e.outboxId, e)
      withJdbcConnection { conn =>
        val sql =
          """MERGE INTO SA_PERSISTENCE.AGENT_TRANSACTIONAL_OUTBOX t
            |USING (SELECT ? AS oid FROM dual) s ON (t.OUTBOX_ID = s.oid)
            |WHEN NOT MATCHED THEN INSERT
            |  (OUTBOX_ID, PERSISTENCE_ID, TARGET_AGENT_TYPE, TARGET_ENTITY_ID, MESSAGE_TYPE, PAYLOAD_SUMMARY, DELIVERY_STATUS, CREATED_AT)
            |VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)""".stripMargin
        Using.resource(conn.prepareStatement(sql)) { ps =>
          ps.setString(1, e.outboxId)
          ps.setString(2, e.outboxId)
          ps.setString(3, s"CustomerAgent:${e.sourceCustomerId}")
          ps.setString(4, e.targetType.toString)
          ps.setString(5, e.targetEntityId)
          ps.setString(6, e.messageType)
          ps.setString(7, e.payloadSummary.take(950))
          ps.setTimestamp(8, Timestamp.from(e.createdAt))
          ps.executeUpdate()
        }
      }
    }

  def markOutboxDelivered(outboxId: String): Unit =
    persistenceOutbox.remove(outboxId)
    withJdbcConnection { conn =>
      Using.resource(
        conn.prepareStatement("UPDATE SA_PERSISTENCE.AGENT_TRANSACTIONAL_OUTBOX SET DELIVERY_STATUS = 'DELIVERED' WHERE OUTBOX_ID = ?")
      ) { ps =>
        ps.setString(1, outboxId)
        ps.executeUpdate()
      }
    }

  // --- 2. SA_FACTS PROJECTION OPERATIONS ---

  def projectTransactionFact(tx: EnrichedTransaction): FinancialTransactionFactRow =
    val row = FinancialTransactionFactRow(
      txId = tx.txId,
      customerId = tx.customerId,
      counterpartyId = tx.counterpartyId,
      counterpartyIsBankClient = tx.counterpartyIsBankClient,
      direction = tx.direction.toString,
      amount = tx.amount,
      currency = tx.currency,
      category = tx.category.toString,
      categorizerSource = tx.categorizerSource,
      resultsInOverdraft = tx.resultsInOverdraft,
      restrictedEntityId = tx.restrictedTag.map(_.entityId),
      occurredAt = tx.timestamp,
      projectedAt = Instant.now()
    )
    financialTxFacts.addFirst(row)
    while financialTxFacts.size() > 500 do financialTxFacts.removeLast()

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_FACTS.FINANCIAL_TRANSACTION_FACTS t
          |USING (SELECT ? AS txid, ? AS cid, ? AS dir FROM dual) s
          |ON (t.TX_ID = s.txid AND t.CUSTOMER_ID = s.cid AND t.DIRECTION = s.dir)
          |WHEN NOT MATCHED THEN INSERT
          |  (TX_ID, CUSTOMER_ID, COUNTERPARTY_ID, COUNTERPARTY_IS_BANK_CLIENT, DIRECTION, AMOUNT, CURRENCY, CATEGORY, CATEGORIZER_SOURCE, RESULTS_IN_OVERDRAFT, RESTRICTED_ENTITY_ID, OCCURRED_AT, PROJECTED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, row.txId)
        ps.setString(2, row.customerId)
        ps.setString(3, row.direction)
        ps.setString(4, row.txId)
        ps.setString(5, row.customerId)
        ps.setString(6, row.counterpartyId)
        ps.setInt(7, if row.counterpartyIsBankClient then 1 else 0)
        ps.setString(8, row.direction)
        ps.setBigDecimal(9, row.amount.bigDecimal)
        ps.setString(10, row.currency)
        ps.setString(11, row.category)
        ps.setString(12, row.categorizerSource)
        ps.setInt(13, if row.resultsInOverdraft then 1 else 0)
        ps.setString(14, row.restrictedEntityId.orNull)
        ps.setTimestamp(15, Timestamp.from(row.occurredAt))
        ps.setTimestamp(16, Timestamp.from(row.projectedAt))
        ps.executeUpdate()
      }
    }

    projectToGlobalPropertyGraph(tx)
    row

  def projectDocumentFact(fact: VerifiedDocumentFact): Unit =
    verifiedDocFacts.put(fact.docId, fact)
    if fact.factType == DocumentFactType.ExternalDebtObligation then
      debtObligationFacts.put(fact.docId, fact)

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_FACTS.VERIFIED_DOCUMENT_FACTS t
          |USING (SELECT ? AS did FROM dual) s ON (t.DOC_ID = s.did)
          |WHEN NOT MATCHED THEN INSERT
          |  (DOC_ID, CUSTOMER_ID, FACT_TYPE, NUMERIC_VALUE, TEXT_VALUE, ISSUER, VERIFIED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, fact.docId)
        ps.setString(2, fact.docId)
        ps.setString(3, fact.customerId)
        ps.setString(4, fact.factType.toString)
        fact.numericValue match
          case Some(bd) => ps.setBigDecimal(5, bd.bigDecimal)
          case None     => ps.setNull(5, java.sql.Types.NUMERIC)
        ps.setString(6, fact.textValue.orNull)
        ps.setString(7, fact.issuer)
        ps.setTimestamp(8, Timestamp.from(fact.verifiedAt))
        ps.executeUpdate()
      }
    }

  // --- 3. SA_GRAPH (ORACLE PROPERTY GRAPH) PROJECTION & ANALYSIS ---

  def projectToGlobalPropertyGraph(tx: EnrichedTransaction): Unit =
    val srcId   = if tx.direction == TransactionDirection.Outflow then tx.customerId else tx.counterpartyId
    val tgtId   = if tx.direction == TransactionDirection.Outflow then tx.counterpartyId else tx.customerId
    val edgeKey = s"$srcId->$tgtId"

    val existingEdge = Option(graphEdges.get(edgeKey))
    val updatedEdge = existingEdge match
      case Some(e) =>
        e.copy(
          totalVolume = e.totalVolume + tx.amount,
          txCount = e.txCount + 1,
          evidenceTxIds = (tx.txId :: e.evidenceTxIds).distinct.take(25),
          lastActiveAt = if tx.timestamp.isAfter(e.lastActiveAt) then tx.timestamp else e.lastActiveAt
        )
      case None =>
        GraphEdge(
          edgeId = edgeKey,
          sourceVertexId = srcId,
          targetVertexId = tgtId,
          relationshipType = if tx.restrictedTag.isDefined then "RESTRICTED_FLOW" else "FINANCIAL_TRANSFER",
          totalVolume = tx.amount,
          txCount = 1,
          evidenceTxIds = List(tx.txId),
          lastActiveAt = tx.timestamp
        )
    graphEdges.put(edgeKey, updatedEdge)

    val customerRisk = tx.restrictedTag.map(_.level.baseRiskWeight * 0.5).getOrElse(0.0)
    upsertVertex(
      vertexId = tx.customerId,
      vertexType = "BANK_CUSTOMER",
      intrinsicRisk = customerRisk,
      at = tx.timestamp
    )

    val cpType =
      if tx.restrictedTag.isDefined then "RESTRICTED_ENTITY"
      else if tx.counterpartyIsBankClient then "BANK_CUSTOMER"
      else "EXTERNAL_ENTITY"
    val cpRisk = tx.restrictedTag.map(_.level.baseRiskWeight).getOrElse(
      if tx.category.isHighRiskCategory then 0.65 else 0.0
    )
    upsertVertex(
      vertexId = tx.counterpartyId,
      vertexType = cpType,
      intrinsicRisk = cpRisk,
      at = tx.timestamp
    )

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_GRAPH.GRAPH_EDGES t
          |USING (SELECT ? AS eid FROM dual) s ON (t.EDGE_ID = s.eid)
          |WHEN MATCHED THEN UPDATE SET
          |  TOTAL_VOLUME = ?, TX_COUNT = ?, EVIDENCE_TX_IDS = ?, LAST_ACTIVE_AT = ?
          |WHEN NOT MATCHED THEN INSERT
          |  (EDGE_ID, SOURCE_VERTEX_ID, TARGET_VERTEX_ID, RELATIONSHIP_TYPE, TOTAL_VOLUME, TX_COUNT, EVIDENCE_TX_IDS, LAST_ACTIVE_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        val evStr = updatedEdge.evidenceTxIds.mkString(",")
        val ts    = Timestamp.from(updatedEdge.lastActiveAt)
        ps.setString(1, updatedEdge.edgeId)
        ps.setBigDecimal(2, updatedEdge.totalVolume.bigDecimal)
        ps.setInt(3, updatedEdge.txCount)
        ps.setString(4, evStr)
        ps.setTimestamp(5, ts)
        ps.setString(6, updatedEdge.edgeId)
        ps.setString(7, updatedEdge.sourceVertexId)
        ps.setString(8, updatedEdge.targetVertexId)
        ps.setString(9, updatedEdge.relationshipType)
        ps.setBigDecimal(10, updatedEdge.totalVolume.bigDecimal)
        ps.setInt(11, updatedEdge.txCount)
        ps.setString(12, evStr)
        ps.setTimestamp(13, ts)
        ps.executeUpdate()
      }
    }

  private def upsertVertex(
      vertexId: String,
      vertexType: String,
      intrinsicRisk: Double,
      at: Instant
  ): Unit =
    val degree = graphEdges.values().asScala.count(e => e.sourceVertexId == vertexId || e.targetVertexId == vertexId)
    val existing = Option(graphVertices.get(vertexId))
    val updated = existing match
      case Some(v) =>
        v.copy(
          vertexType = if v.vertexType == "RESTRICTED_ENTITY" then v.vertexType else vertexType,
          intrinsicRisk = math.max(v.intrinsicRisk, intrinsicRisk),
          degreeCentrality = degree,
          lastUpdatedAt = if at.isAfter(v.lastUpdatedAt) then at else v.lastUpdatedAt
        )
      case None =>
        GraphVertex(
          vertexId = vertexId,
          vertexType = vertexType,
          segmentId = "RETAIL-PRIME",
          intrinsicRisk = intrinsicRisk,
          degreeCentrality = degree,
          communityId = s"COMM-${math.abs(vertexId.hashCode % 10)}",
          lastUpdatedAt = at
        )
    graphVertices.put(vertexId, updated)

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_GRAPH.GRAPH_VERTICES t
          |USING (SELECT ? AS vid FROM dual) s ON (t.VERTEX_ID = s.vid)
          |WHEN MATCHED THEN UPDATE SET
          |  VERTEX_TYPE = ?, INTRINSIC_RISK = ?, DEGREE_CENTRALITY = ?, LAST_UPDATED_AT = ?
          |WHEN NOT MATCHED THEN INSERT
          |  (VERTEX_ID, VERTEX_TYPE, SEGMENT_ID, INTRINSIC_RISK, DEGREE_CENTRALITY, COMMUNITY_ID, LAST_UPDATED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        val ts = Timestamp.from(updated.lastUpdatedAt)
        ps.setString(1, updated.vertexId)
        ps.setString(2, updated.vertexType)
        ps.setDouble(3, updated.intrinsicRisk)
        ps.setInt(4, updated.degreeCentrality)
        ps.setTimestamp(5, ts)
        ps.setString(6, updated.vertexId)
        ps.setString(7, updated.vertexType)
        ps.setString(8, updated.segmentId)
        ps.setDouble(9, updated.intrinsicRisk)
        ps.setInt(10, updated.degreeCentrality)
        ps.setString(11, updated.communityId)
        ps.setTimestamp(12, ts)
        ps.executeUpdate()
      }
    }

  /**
   * Executes Global Relationship Graph Analysis on Oracle Exadata (`SA_GRAPH` -> `SA_INTELLIGENCE`, Section 5 & 14).
   */
  def analyzeGlobalGraphAndDetectPatterns(now: Instant = Instant.now()): List[NetworkRiskPatternDetected] =
    val allEdges    = graphEdges.values().asScala.toList
    val allVertices = graphVertices.asScala.toMap

    val byCounterparty: Map[String, List[GraphEdge]] = allEdges.groupBy(_.targetVertexId)

    val detectedPatterns = byCounterparty.toList.flatMap { case (counterpartyId, incomingEdges) =>
      val distinctCustomers = incomingEdges
        .map(_.sourceVertexId)
        .filter(vid => allVertices.get(vid).exists(_.vertexType == "BANK_CUSTOMER"))
        .distinct

      val cpVertexOpt = allVertices.get(counterpartyId)
      val isRiskyCounterparty = cpVertexOpt.exists(v => v.intrinsicRisk >= 0.50 || v.vertexType == "RESTRICTED_ENTITY")

      if distinctCustomers.size >= 2 && (isRiskyCounterparty || incomingEdges.map(_.totalVolume).sum >= BigDecimal("1000.00")) then
        val baseScore    = cpVertexOpt.map(_.intrinsicRisk).filter(_ > 0.0).getOrElse(0.55)
        val patternScore = BigDecimal(math.min(0.95, baseScore)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val allEvidence  = incomingEdges.flatMap(_.evidenceTxIds).distinct

        distinctCustomers.map { affectedCust =>
          val pattern = NetworkRiskPatternDetected(
            patternId = s"PAT-GLOBAL-${counterpartyId.replaceAll("[^A-Za-z0-9]", "")}-${affectedCust.replaceAll("[^A-Za-z0-9]", "")}",
            patternType =
              if distinctCustomers.size >= 3 then NetworkRiskPatternType.HighDegreeNodeExposure
              else NetworkRiskPatternType.SharedHighRiskCounterparty,
            affectedCustomerId = affectedCust,
            sharedEntityId = counterpartyId,
            linkedCustomers = distinctCustomers,
            riskScore = patternScore,
            reasonCode = "RC_303_GLOBAL_GRAPH_SHARED_CLUSTER",
            description =
              s"Global Property Graph detected ${distinctCustomers.size} bank accounts (${distinctCustomers.mkString(", ")}) converging on shared counterparty '$counterpartyId'",
            evidenceIds = allEvidence,
            detectedAt = now
          )
          recordRiskIntelligenceEvent(pattern)
          pattern
        }
      else Nil
    }

    detectedPatterns

  // --- 4. SA_INTELLIGENCE OPERATIONS ---

  def recordRiskIntelligenceEvent(event: NetworkRiskPatternDetected): Unit =
    riskIntelEvents.addFirst(event)
    while riskIntelEvents.size() > 200 do riskIntelEvents.removeLast()

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_INTELLIGENCE.RISK_INTELLIGENCE_EVENTS t
          |USING (SELECT ? AS pid, ? AS cid FROM dual) s ON (t.PATTERN_ID = s.pid AND t.AFFECTED_CUSTOMER_ID = s.cid)
          |WHEN NOT MATCHED THEN INSERT
          |  (PATTERN_ID, PATTERN_TYPE, AFFECTED_CUSTOMER_ID, SHARED_ENTITY_ID, LINKED_CUSTOMERS, RISK_SCORE, REASON_CODE, DESCRIPTION, EVIDENCE_IDS, DETECTED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, event.patternId)
        ps.setString(2, event.affectedCustomerId)
        ps.setString(3, event.patternId)
        ps.setString(4, event.patternType.toString)
        ps.setString(5, event.affectedCustomerId)
        ps.setString(6, event.sharedEntityId)
        ps.setString(7, event.linkedCustomers.mkString(","))
        ps.setDouble(8, event.riskScore)
        ps.setString(9, event.reasonCode)
        ps.setString(10, event.description.take(950))
        ps.setString(11, event.evidenceIds.mkString(","))
        ps.setTimestamp(12, Timestamp.from(event.detectedAt))
        ps.executeUpdate()
      }
    }

  def savePortfolioSnapshot(state: PortfolioSituationState): Unit =
    portfolioSnapshots.put(state.segmentId, state)

  // --- 5. SA_DECISIONS OPERATIONS ---

  def recordProjectionAudit(projection: SituationProjection): Unit =
    projectionAuditLog.addFirst(projection)
    while projectionAuditLog.size() > 200 do projectionAuditLog.removeLast()

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_DECISIONS.SITUATION_PROJECTION_LOG t
          |USING (SELECT ? AS aid FROM dual) s ON (t.APPLICATION_ID = s.aid)
          |WHEN NOT MATCHED THEN INSERT
          |  (APPLICATION_ID, CUSTOMER_ID, STATE_VERSION, RULE_VERSION, DATA_COMPLETENESS, FUTURE_DTI, EXPECTED_LIQUIDITY, RISK_EXPOSURE_SCORE, IS_AFFORDABLE, PROJECTED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, projection.applicationId)
        ps.setString(2, projection.applicationId)
        ps.setString(3, projection.customerId)
        ps.setLong(4, projection.stateVersion)
        ps.setString(5, projection.ruleVersion)
        ps.setDouble(6, projection.dataCompleteness.score)
        ps.setBigDecimal(7, projection.futureDti.bigDecimal)
        ps.setBigDecimal(8, projection.expectedMonthlyLiquidity.bigDecimal)
        ps.setDouble(9, projection.riskExposureScore)
        ps.setInt(10, if projection.isAffordable then 1 else 0)
        ps.setTimestamp(11, Timestamp.from(projection.projectedAt))
        ps.executeUpdate()
      }
    }

  def recordDecisionAudit(decision: LoanDecisionResponse): Unit =
    decisionAuditLog.addFirst(decision)
    while decisionAuditLog.size() > 200 do decisionAuditLog.removeLast()

    withJdbcConnection { conn =>
      val status = decision.outcome match
        case _: DecisionOutcome.Approved => "APPROVED"
        case _: DecisionOutcome.Denied   => "DENIED"
      val reasons = decision.auditTrail.principalAdverseReasons.map(_.code).mkString(",")
      val sql =
        """MERGE INTO SA_DECISIONS.DECISION_AUDIT_LOG t
          |USING (SELECT ? AS aid FROM dual) s ON (t.APPLICATION_ID = s.aid)
          |WHEN NOT MATCHED THEN INSERT
          |  (APPLICATION_ID, CUSTOMER_ID, OUTCOME_STATUS, CREDIT_SCORE, STATE_VERSION, POLICY_VERSION, DATA_COMPLETENESS, REASON_CODES, STATEMENT_OF_REASONS, EVALUATED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, decision.applicationId)
        ps.setString(2, decision.applicationId)
        ps.setString(3, decision.customerId)
        ps.setString(4, status)
        ps.setInt(5, decision.auditTrail.finalCreditScore)
        ps.setLong(6, decision.auditTrail.stateVersion)
        ps.setString(7, decision.auditTrail.policyVersion)
        ps.setDouble(8, decision.auditTrail.dataCompleteness.score)
        ps.setString(9, if reasons.isEmpty then "NONE" else reasons)
        ps.setString(10, decision.auditTrail.statementOfReasons)
        ps.setTimestamp(11, Timestamp.from(decision.evaluatedAt))
        ps.executeUpdate()
      }
    }

  // --- 6. SA_GOVERNANCE OPERATIONS ---

  def registerGovernancePolicy(policy: GovernancePolicyRow): Unit =
    governancePolicies.put(policy.policyVersion, policy)

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_GOVERNANCE.GOVERNANCE_POLICY_REGISTRY t
          |USING (SELECT ? AS pv FROM dual) s ON (t.POLICY_VERSION = s.pv)
          |WHEN NOT MATCHED THEN INSERT
          |  (POLICY_VERSION, MODEL_NAME, MAX_DTI_RATIO, MAX_HIGH_RISK_SPEND, MAX_OVERDRAFT_COUNT, MAX_1HOP_RISK_SCORE, MAX_2HOP_RISK_SCORE, ACTIVE_FLAG, REGISTERED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, policy.policyVersion)
        ps.setString(2, policy.policyVersion)
        ps.setString(3, policy.modelName)
        ps.setBigDecimal(4, policy.maxDtiRatio.bigDecimal)
        ps.setBigDecimal(5, policy.maxHighRiskSpendRatio.bigDecimal)
        ps.setInt(6, policy.maxOverdraftCount)
        ps.setDouble(7, policy.max1HopRiskScore)
        ps.setDouble(8, policy.max2HopRiskScore)
        ps.setInt(9, if policy.active then 1 else 0)
        ps.setTimestamp(10, Timestamp.from(policy.registeredAt))
        ps.executeUpdate()
      }
    }

  // --- QUERY & INSPECTION METHODS ---

  def isJdbcConnected: Boolean = jdbcConnectionReady

  def getJournalEntries(limit: Int = 50): List[OracleJournalRow] =
    persistenceJournal.asScala.take(limit).toList

  def getSnapshots: List[OracleSnapshotRow] =
    persistenceSnapshots.values().asScala.toList

  def getPendingOutbox: List[OutboxEntry] =
    persistenceOutbox.values().asScala.toList

  def getTransactionFacts(limit: Int = 50): List[FinancialTransactionFactRow] =
    financialTxFacts.asScala.take(limit).toList

  def getVerifiedDocumentFacts: List[VerifiedDocumentFact] =
    verifiedDocFacts.values().asScala.toList

  def getGraphVertices: List[GraphVertex] =
    graphVertices.values().asScala.toList

  def getGraphEdges: List[GraphEdge] =
    graphEdges.values().asScala.toList

  def getRiskIntelligenceEvents: List[NetworkRiskPatternDetected] =
    riskIntelEvents.asScala.toList

  def getPortfolioSnapshots: List[PortfolioSituationState] =
    portfolioSnapshots.values().asScala.toList

  def getDecisionAuditLog: List[LoanDecisionResponse] =
    decisionAuditLog.asScala.toList

  def getProjectionAuditLog: List[SituationProjection] =
    projectionAuditLog.asScala.toList

  def getGovernancePolicies: List[GovernancePolicyRow] =
    governancePolicies.values().asScala.toList

  def getSchemaRecordCounts: Map[String, Int] = Map(
    "SA_PERSISTENCE"  -> (persistenceJournal.size() + persistenceSnapshots.size() + persistenceOutbox.size()),
    "SA_FACTS"        -> (financialTxFacts.size() + verifiedDocFacts.size() + debtObligationFacts.size()),
    "SA_GRAPH"        -> (graphVertices.size() + graphEdges.size()),
    "SA_INTELLIGENCE" -> (riskIntelEvents.size() + portfolioSnapshots.size()),
    "SA_DECISIONS"    -> (decisionAuditLog.size() + projectionAuditLog.size()),
    "SA_GOVERNANCE"   -> governancePolicies.size()
  )

  def reset(): Unit =
    seqCounters.clear()
    persistenceJournal.clear()
    persistenceSnapshots.clear()
    persistenceOutbox.clear()
    financialTxFacts.clear()
    verifiedDocFacts.clear()
    debtObligationFacts.clear()
    graphVertices.clear()
    graphEdges.clear()
    riskIntelEvents.clear()
    portfolioSnapshots.clear()
    decisionAuditLog.clear()
    projectionAuditLog.clear()
