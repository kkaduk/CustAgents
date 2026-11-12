package com.bank.sa.oracle

import com.bank.sa.domain.*
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, Timestamp}
import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
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
      sys.env.get("ORACLE_PASSWORD").orElse(sys.props.get("oracle.password")).getOrElse("PKO_666#"),
    maxPoolSize: Int = 8
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
  private val financialTxFacts       = new ConcurrentLinkedDeque[FinancialTransactionFactRow]()
  private val verifiedDocFacts       = new ConcurrentHashMap[String, VerifiedDocumentFact]()
  private val debtObligationFacts    = new ConcurrentHashMap[String, VerifiedDocumentFact]()
  private val customerAccounts       = new ConcurrentHashMap[String, CustomerAccount]()
  private val accountFirstSeenMap    = new ConcurrentHashMap[String, AccountFirstSeen]()
  private val accountHistoryMap      = new ConcurrentHashMap[String, AccountVerificationResult]()
  private val employeeRegistry       = new ConcurrentHashMap[String, EmployeeProfile]()
  private val contactChangeHistory   = new ConcurrentLinkedDeque[NormalizedCustomerContactChange]()

  // 3. SA_GRAPH (Oracle Property Graph: Vertices & Edges)
  private val graphVertices          = new ConcurrentHashMap[String, GraphVertex]()
  private val graphEdges             = new ConcurrentHashMap[String, GraphEdge]()

  // 4. SA_INTELLIGENCE (Global Risk Intelligence & Portfolio Analytics)
  private val riskIntelEvents        = new ConcurrentLinkedDeque[NetworkRiskPatternDetected]()
  private val portfolioSnapshots     = new ConcurrentHashMap[String, PortfolioSituationState]()

  // 5. SA_DECISIONS (Persisted Evaluations, Projections & Pre-Execution Transaction Authorizations)
  private val decisionAuditLog       = new ConcurrentLinkedDeque[LoanDecisionResponse]()
  private val projectionAuditLog     = new ConcurrentLinkedDeque[SituationProjection]()
  private val txAuthorizationAuditLog = new ConcurrentLinkedDeque[TransactionAuthorizationResponse]()

  // 6. SA_GOVERNANCE (Rule Versions, Models, Policies)
  private val governancePolicies   = new ConcurrentHashMap[String, GovernancePolicyRow]()

  // Bounded JDBC Connection Pool (Roadmap 9.1: avoids DriverManager.getConnection per event)
  private val connectionPool       = new LinkedBlockingQueue[Connection](math.max(1, jdbcConfig.maxPoolSize))

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

  private def acquirePooledConnection(): Connection =
    val pooled = connectionPool.poll()
    if pooled != null then
      try
        if !pooled.isClosed && pooled.isValid(2) then pooled
        else
          try pooled.close()
          catch case NonFatal(_) => ()
          DriverManager.getConnection(jdbcConfig.jdbcUrl, jdbcConfig.user, jdbcConfig.password)
      catch
        case NonFatal(_) =>
          DriverManager.getConnection(jdbcConfig.jdbcUrl, jdbcConfig.user, jdbcConfig.password)
    else
      DriverManager.getConnection(jdbcConfig.jdbcUrl, jdbcConfig.user, jdbcConfig.password)

  private def releasePooledConnection(conn: Connection): Unit =
    if conn != null then
      try
        if !conn.isClosed && !connectionPool.offer(conn) then conn.close()
      catch
        case NonFatal(_) =>
          try conn.close()
          catch case NonFatal(_) => ()

  def closeConnectionPool(): Unit =
    var conn = connectionPool.poll()
    while conn != null do
      try conn.close()
      catch case NonFatal(_) => ()
      conn = connectionPool.poll()

  def pooledIdleConnectionsCount: Int = connectionPool.size()

  private def withJdbcConnection[A](f: Connection => A): Option[A] =
    if !jdbcConfig.enabled || !jdbcConnectionReady then None
    else
      var conn: Connection = null
      try
        conn = acquirePooledConnection()
        Some(f(conn))
      catch
        case NonFatal(ex) =>
          log.warn("Oracle JDBC operation warning: {}", ex.getMessage)
          None
      finally
        releasePooledConnection(conn)

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
          """CREATE TABLE SA_FACTS.ACCOUNT_HISTORY_REGISTRY (
            |  ACCOUNT_ID VARCHAR2(128 CHAR) NOT NULL,
            |  OWNER_CUSTOMER_ID VARCHAR2(128 CHAR),
            |  FIRST_SEEN_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  LAST_SEEN_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  TX_COUNT NUMBER(10) NOT NULL,
            |  TOTAL_VOLUME NUMBER(19, 4) NOT NULL,
            |  LINKED_CUSTOMERS VARCHAR2(2000 CHAR),
            |  HIGH_RISK_CONNECTIONS VARCHAR2(2000 CHAR),
            |  CONSTRAINT PK_SA_ACCOUNT_HISTORY PRIMARY KEY (ACCOUNT_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_FACTS.CONTACT_CHANGE_HISTORY (
            |  EVENT_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  PREV_PHONE VARCHAR2(64 CHAR),
            |  NEW_PHONE VARCHAR2(64 CHAR) NOT NULL,
            |  MATCHED_EMPLOYEE_ID VARCHAR2(128 CHAR),
            |  SERVICES_CUSTOMER NUMBER(1) NOT NULL,
            |  SOURCE_SYSTEM VARCHAR2(128 CHAR) NOT NULL,
            |  OCCURRED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_CONTACT_CHANGE PRIMARY KEY (EVENT_ID)
            |)""".stripMargin,
          """CREATE TABLE SA_DECISIONS.TRANSACTION_AUTHORIZATION_LOG (
            |  TRANSFER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  CUSTOMER_ID VARCHAR2(128 CHAR) NOT NULL,
            |  TO_ACCOUNT_ID VARCHAR2(128 CHAR) NOT NULL,
            |  TO_COUNTERPARTY_ID VARCHAR2(128 CHAR) NOT NULL,
            |  AMOUNT NUMBER(19, 4) NOT NULL,
            |  DECISION_STATUS VARCHAR2(32 CHAR) NOT NULL,
            |  RISK_SCORE NUMBER(10) NOT NULL,
            |  REASON_CODES VARCHAR2(1000 CHAR),
            |  RAIL_DIRECTIVE VARCHAR2(128 CHAR) NOT NULL,
            |  EVALUATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
            |  CONSTRAINT PK_SA_TX_AUTH_LOG PRIMARY KEY (TRANSFER_ID)
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
            |)""".stripMargin,
          """CREATE PROPERTY GRAPH SA_GRAPH.SA_GLOBAL_RELATIONSHIP_GRAPH
            |  VERTEX TABLES (
            |    SA_GRAPH.GRAPH_VERTICES KEY (VERTEX_ID) PROPERTIES ARE ALL COLUMNS
            |  )
            |  EDGE TABLES (
            |    SA_GRAPH.GRAPH_EDGES KEY (EDGE_ID)
            |      SOURCE KEY (SOURCE_VERTEX_ID) REFERENCES GRAPH_VERTICES (VERTEX_ID)
            |      DESTINATION KEY (TARGET_VERTEX_ID) REFERENCES GRAPH_VERTICES (VERTEX_ID)
            |      PROPERTIES ARE ALL COLUMNS
            |  )""".stripMargin
        )

        ddlStatements.foreach(sql => execDdlIgnoreExists(conn, sql))
        jdbcConnectionReady = true
        log.info("Oracle Exadata 6-Schema + SQL Property Graph initialization complete.")
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
    appendJournalEventsBatch(persistenceId, List((eventType, payloadSummary, timestamp))).head

  /**
   * Batched JDBC journal append (`PreparedStatement.addBatch` / `executeBatch`, Roadmap 9.1).
   * Persists multiple journal events (e.g., `ObservedEventPersisted` + `SituationEvaluated`) in a single Oracle round-trip.
   */
  def appendJournalEventsBatch(
      persistenceId: String,
      events: Seq[(String, String, Instant)]
  ): List[OracleJournalRow] =
    val rows = events.toList.map { case (eventType, payloadSummary, timestamp) =>
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
      row
    }

    if rows.nonEmpty then
      withJdbcConnection { conn =>
        val sql =
          """MERGE INTO SA_PERSISTENCE.AGENT_EVENT_JOURNAL t
            |USING (SELECT ? AS pid, ? AS seq FROM dual) s
            |ON (t.PERSISTENCE_ID = s.pid AND t.SEQUENCE_NR = s.seq)
            |WHEN NOT MATCHED THEN INSERT (PERSISTENCE_ID, SEQUENCE_NR, EVENT_TYPE, EVENT_PAYLOAD, EVENT_TIMESTAMP, EVENT_SCHEMA_VERSION)
            |VALUES (?, ?, ?, ?, ?, 1)""".stripMargin
        Using.resource(conn.prepareStatement(sql)) { ps =>
          rows.foreach { row =>
            ps.setString(1, row.persistenceId)
            ps.setLong(2, row.sequenceNr)
            ps.setString(3, row.persistenceId)
            ps.setLong(4, row.sequenceNr)
            ps.setString(5, row.eventType)
            ps.setString(6, row.eventPayloadSummary.take(1900))
            ps.setTimestamp(7, Timestamp.from(row.eventTimestamp))
            ps.addBatch()
          }
          ps.executeBatch()
        }
      }
    rows

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

  // --- 2b. ACCOUNT OWNERSHIP, 1 PLN ACCOUNT HISTORY & EMPLOYEE/CONTACT OPERATIONS (Doc/NowePotrzeby.md Sections 2, 3.3, 4) ---

  def registerCustomerAccount(account: CustomerAccount): CustomerAccount =
    customerAccounts.put(account.accountId, account)
    upsertVertex(account.accountId, "CUSTOMER_ACCOUNT", 0.0, account.openedAt)
    upsertVertex(account.customerId, "BANK_CUSTOMER", 0.0, account.openedAt)
    upsertEdge(
      edgeId = s"OWNS:${account.customerId}->${account.accountId}",
      sourceVertexId = account.customerId,
      targetVertexId = account.accountId,
      relationshipType = "CUSTOMER_OWNS_ACCOUNT",
      volumeDelta = BigDecimal(0),
      txCountDelta = 0,
      evidenceIdOpt = Some(s"ACC-OPEN-${account.accountId}"),
      at = account.openedAt,
      validFrom = Some(account.openedAt),
      sourceSystem = "CORE_BANKING_ACCOUNTS"
    )
    recordAccountObservation(
      accountId = account.accountId,
      ownerCustomerIdOpt = Some(account.customerId),
      linkedCustomerOpt = Some(account.customerId),
      txIdOpt = None,
      amount = BigDecimal(0),
      highRiskEntityOpt = None,
      observedAt = account.openedAt
    )
    account

  def registerEmployee(emp: EmployeeProfile): EmployeeProfile =
    employeeRegistry.put(emp.employeeId, emp)
    val phoneVertexId = s"PHONE:${emp.phoneNumberNormalized}"
    upsertVertex(emp.employeeId, "EMPLOYEE", 0.0, emp.registeredAt)
    upsertVertex(phoneVertexId, "PHONE_NUMBER", 0.0, emp.registeredAt)
    upsertEdge(
      edgeId = s"EMP_PHONE:${emp.employeeId}->$phoneVertexId",
      sourceVertexId = emp.employeeId,
      targetVertexId = phoneVertexId,
      relationshipType = "EMPLOYEE_HAS_PHONE",
      volumeDelta = BigDecimal(0),
      txCountDelta = 1,
      evidenceIdOpt = Some(s"HR-REG-${emp.employeeId}"),
      at = emp.registeredAt,
      validFrom = Some(emp.registeredAt),
      sourceSystem = "HR_DIRECTORY"
    )
    emp.servicedCustomerIds.foreach { cid =>
      upsertVertex(cid, "BANK_CUSTOMER", 0.0, emp.registeredAt)
      upsertEdge(
        edgeId = s"EMP_SERVICES:${emp.employeeId}->$cid",
        sourceVertexId = emp.employeeId,
        targetVertexId = cid,
        relationshipType = "EMPLOYEE_SERVICES_CUSTOMER",
        volumeDelta = BigDecimal(0),
        txCountDelta = 1,
        evidenceIdOpt = Some(s"CRM-ASSIGN-${emp.employeeId}-$cid"),
        at = emp.registeredAt,
        validFrom = Some(emp.registeredAt),
        sourceSystem = "CRM_ASSIGNMENT"
      )
    }
    emp

  def projectContactChange(change: NormalizedCustomerContactChange): NormalizedCustomerContactChange =
    contactChangeHistory.addFirst(change)
    while contactChangeHistory.size() > 200 do contactChangeHistory.removeLast()

    val newPhoneVertexId = s"PHONE:${change.newPhoneNormalized}"
    val phoneRisk        = if change.matchedEmployeeId.isDefined then 0.65 else 0.0
    upsertVertex(change.customerId, "BANK_CUSTOMER", phoneRisk * 0.5, change.occurredAt)
    upsertVertex(newPhoneVertexId, "PHONE_NUMBER", phoneRisk, change.occurredAt)

    change.previousPhoneNormalized.filter(_.nonEmpty).foreach { prevPhone =>
      val prevPhoneVertexId = s"PHONE:$prevPhone"
      upsertVertex(prevPhoneVertexId, "PHONE_NUMBER", 0.0, change.occurredAt)
      val prevEdgeKey = s"CUST_PHONE:${change.customerId}->$prevPhoneVertexId"
      Option(graphEdges.get(prevEdgeKey)).foreach { existing =>
        graphEdges.put(prevEdgeKey, existing.copy(validTo = Some(change.occurredAt)))
      }
      upsertEdge(
        edgeId = s"PHONE_CHANGE:${change.customerId}:$prevPhoneVertexId->$newPhoneVertexId",
        sourceVertexId = prevPhoneVertexId,
        targetVertexId = newPhoneVertexId,
        relationshipType = "CUSTOMER_PHONE_CHANGED",
        volumeDelta = BigDecimal(0),
        txCountDelta = 1,
        evidenceIdOpt = Some(change.eventId),
        at = change.occurredAt,
        validFrom = Some(change.occurredAt),
        sourceSystem = change.sourceSystem,
        lastEventId = Some(change.eventId)
      )
    }

    upsertEdge(
      edgeId = s"CUST_PHONE:${change.customerId}->$newPhoneVertexId",
      sourceVertexId = change.customerId,
      targetVertexId = newPhoneVertexId,
      relationshipType = "CUSTOMER_HAS_PHONE",
      volumeDelta = BigDecimal(0),
      txCountDelta = 1,
      evidenceIdOpt = Some(change.eventId),
      at = change.occurredAt,
      validFrom = Some(change.occurredAt),
      sourceSystem = change.sourceSystem,
      lastEventId = Some(change.eventId)
    )

    withJdbcConnection { conn =>
      val sql =
        """MERGE INTO SA_FACTS.CONTACT_CHANGE_HISTORY t
          |USING (SELECT ? AS eid FROM dual) s ON (t.EVENT_ID = s.eid)
          |WHEN NOT MATCHED THEN INSERT
          |  (EVENT_ID, CUSTOMER_ID, PREV_PHONE, NEW_PHONE, MATCHED_EMPLOYEE_ID, SERVICES_CUSTOMER, SOURCE_SYSTEM, OCCURRED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, change.eventId)
        ps.setString(2, change.eventId)
        ps.setString(3, change.customerId)
        ps.setString(4, change.previousPhoneNormalized.orNull)
        ps.setString(5, change.newPhoneNormalized)
        ps.setString(6, change.matchedEmployeeId.orNull)
        ps.setInt(7, if change.employeeServicesCustomer then 1 else 0)
        ps.setString(8, change.sourceSystem)
        ps.setTimestamp(9, Timestamp.from(change.occurredAt))
        ps.executeUpdate()
      }
    }
    change

  private def recordAccountObservation(
      accountId: String,
      ownerCustomerIdOpt: Option[String],
      linkedCustomerOpt: Option[String],
      txIdOpt: Option[String],
      amount: BigDecimal,
      highRiskEntityOpt: Option[String],
      observedAt: Instant
  ): Unit =
    if accountId.nonEmpty then
      accountFirstSeenMap.computeIfAbsent(
        accountId,
        _ =>
          AccountFirstSeen(
            accountId = accountId,
            firstObservedTxId = txIdOpt.getOrElse("ACCOUNT_REGISTRATION"),
            firstObservedCustomerId = ownerCustomerIdOpt.orElse(linkedCustomerOpt).getOrElse("UNKNOWN"),
            firstSeenAt = observedAt
          )
      )
      val firstSeen = accountFirstSeenMap.get(accountId)
      val existing  = Option(accountHistoryMap.get(accountId))
      val updated = existing match
        case Some(prev) =>
          val mergedOwner = prev.ownerCustomerId.orElse(ownerCustomerIdOpt).orElse(Option(customerAccounts.get(accountId)).map(_.customerId))
          val mergedLinked = (prev.linkedCustomers ++ linkedCustomerOpt.toList ++ ownerCustomerIdOpt.toList).distinct.take(25)
          val mergedRisk   = (prev.highRiskConnections ++ highRiskEntityOpt.toList).distinct.take(25)
          val mergedSuspTx =
            if highRiskEntityOpt.isDefined then (txIdOpt.toList ++ prev.suspiciousTxIds).distinct.take(25)
            else prev.suspiciousTxIds
          val earliest = if observedAt.isBefore(prev.firstSeenAt.getOrElse(observedAt)) then observedAt else prev.firstSeenAt.getOrElse(observedAt)
          val latest   = if observedAt.isAfter(prev.lastSeenAt.getOrElse(observedAt)) then observedAt else prev.lastSeenAt.getOrElse(observedAt)
          val txCount  = if txIdOpt.isDefined then prev.transactionCount + 1 else prev.transactionCount
          prev.copy(
            knownInBank = true,
            ownerCustomerId = mergedOwner,
            firstSeenAt = Some(earliest),
            lastSeenAt = Some(latest),
            transactionCount = txCount,
            totalVolume = prev.totalVolume + amount,
            linkedCustomers = mergedLinked,
            highRiskConnections = mergedRisk,
            suspiciousTxIds = mergedSuspTx,
            assessment =
              if mergedRisk.nonEmpty then s"Rachunek znany w banku, lecz powiązany z podmiotami podwyższonego ryzyka (${mergedRisk.mkString(", ")})"
              else "Rachunek znany w banku — potwierdzona historia transakcji",
            riskSignalCode = if mergedRisk.nonEmpty then Some("SUSPECTED_MULE_ACCOUNT") else None,
            checkedAt = latest
          )
        case None =>
          val owner = ownerCustomerIdOpt.orElse(Option(customerAccounts.get(accountId)).map(_.customerId))
          val riskList = highRiskEntityOpt.toList
          AccountVerificationResult(
            accountId = accountId,
            knownInBank = true,
            ownerCustomerId = owner,
            firstSeenAt = Some(firstSeen.firstSeenAt),
            lastSeenAt = Some(observedAt),
            transactionCount = if txIdOpt.isDefined then 1 else 0,
            totalVolume = amount,
            linkedCustomers = (linkedCustomerOpt.toList ++ owner.toList).distinct,
            highRiskConnections = riskList,
            suspiciousTxIds = if highRiskEntityOpt.isDefined then txIdOpt.toList else Nil,
            assessment =
              if riskList.nonEmpty then s"Rachunek powiązany z podmiotem wysokiego ryzyka (${riskList.mkString(", ")})"
              else "Rachunek znany w banku — potwierdzona historia transakcji",
            riskSignalCode = if riskList.nonEmpty then Some("SUSPECTED_MULE_ACCOUNT") else None,
            checkedAt = observedAt
          )
      accountHistoryMap.put(accountId, updated)

      withJdbcConnection { conn =>
        val sql =
          """MERGE INTO SA_FACTS.ACCOUNT_HISTORY_REGISTRY t
            |USING (SELECT ? AS aid FROM dual) s ON (t.ACCOUNT_ID = s.aid)
            |WHEN MATCHED THEN UPDATE SET
            |  OWNER_CUSTOMER_ID = ?, LAST_SEEN_AT = ?, TX_COUNT = ?, TOTAL_VOLUME = ?, LINKED_CUSTOMERS = ?, HIGH_RISK_CONNECTIONS = ?
            |WHEN NOT MATCHED THEN INSERT
            |  (ACCOUNT_ID, OWNER_CUSTOMER_ID, FIRST_SEEN_AT, LAST_SEEN_AT, TX_COUNT, TOTAL_VOLUME, LINKED_CUSTOMERS, HIGH_RISK_CONNECTIONS)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
        Using.resource(conn.prepareStatement(sql)) { ps =>
          val fTs = Timestamp.from(updated.firstSeenAt.getOrElse(observedAt))
          val lTs = Timestamp.from(updated.lastSeenAt.getOrElse(observedAt))
          val linkedStr = updated.linkedCustomers.mkString(",")
          val riskStr   = updated.highRiskConnections.mkString(",")
          ps.setString(1, accountId)
          ps.setString(2, updated.ownerCustomerId.orNull)
          ps.setTimestamp(3, lTs)
          ps.setInt(4, updated.transactionCount)
          ps.setBigDecimal(5, updated.totalVolume.bigDecimal)
          ps.setString(6, linkedStr)
          ps.setString(7, riskStr)
          ps.setString(8, accountId)
          ps.setString(9, updated.ownerCustomerId.orNull)
          ps.setTimestamp(10, fTs)
          ps.setTimestamp(11, lTs)
          ps.setInt(12, updated.transactionCount)
          ps.setBigDecimal(13, updated.totalVolume.bigDecimal)
          ps.setString(14, linkedStr)
          ps.setString(15, riskStr)
          ps.executeUpdate()
        }
      }

  /**
   * Verifies account history for a 1 PLN verification transfer or pre-execution payment check (`Doc/NowePotrzeby.md` Section 4).
   * Distinguishes a known account with history from an account never previously observed by the bank (`UNKNOWN_ACCOUNT_HISTORY`).
   */
  def verifyAccountHistory(accountId: String, now: Instant = Instant.now()): AccountVerificationResult =
    Option(accountHistoryMap.get(accountId)) match
      case Some(existing) =>
        existing.copy(checkedAt = now)
      case None =>
        AccountVerificationResult(
          accountId = accountId,
          knownInBank = false,
          ownerCustomerId = None,
          firstSeenAt = None,
          lastSeenAt = None,
          transactionCount = 0,
          totalVolume = BigDecimal(0),
          linkedCustomers = Nil,
          highRiskConnections = Nil,
          suspiciousTxIds = Nil,
          assessment = "Nowy / nieznany rachunek (Brak w dostępnej historii banku — brak historii nie dowodzi daty utworzenia rachunku)",
          riskSignalCode = Some("UNKNOWN_ACCOUNT_HISTORY"),
          checkedAt = now
        )

  // --- 3. SA_GRAPH (ORACLE PROPERTY GRAPH) PROJECTION & ANALYSIS ---

  private def upsertEdge(
      edgeId: String,
      sourceVertexId: String,
      targetVertexId: String,
      relationshipType: String,
      volumeDelta: BigDecimal,
      txCountDelta: Int,
      evidenceIdOpt: Option[String],
      at: Instant,
      validFrom: Option[Instant] = None,
      validTo: Option[Instant] = None,
      sourceSystem: String = "PAYMENT_RAILS",
      lastEventId: Option[String] = None
  ): GraphEdge =
    val existingEdge = Option(graphEdges.get(edgeId))
    val updatedEdge = existingEdge match
      case Some(e) =>
        e.copy(
          totalVolume = e.totalVolume + volumeDelta,
          txCount = e.txCount + txCountDelta,
          evidenceTxIds = (evidenceIdOpt.toList ++ e.evidenceTxIds).distinct.take(25),
          lastActiveAt = if at.isAfter(e.lastActiveAt) then at else e.lastActiveAt,
          validFrom = e.validFrom.orElse(validFrom),
          validTo = validTo.orElse(e.validTo),
          sourceSystem = sourceSystem,
          lastEventId = lastEventId.orElse(evidenceIdOpt).orElse(e.lastEventId)
        )
      case None =>
        GraphEdge(
          edgeId = edgeId,
          sourceVertexId = sourceVertexId,
          targetVertexId = targetVertexId,
          relationshipType = relationshipType,
          totalVolume = volumeDelta,
          txCount = math.max(1, txCountDelta),
          evidenceTxIds = evidenceIdOpt.toList,
          lastActiveAt = at,
          validFrom = validFrom.orElse(Some(at)),
          validTo = validTo,
          sourceSystem = sourceSystem,
          lastEventId = lastEventId.orElse(evidenceIdOpt)
        )
    graphEdges.put(edgeId, updatedEdge)

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
    updatedEdge

  def projectToGlobalPropertyGraph(tx: EnrichedTransaction): Unit =
    val isOwnTransfer = tx.isOwnAccountTransfer || tx.category == TransactionCategory.InternalOwnAccountTransfer || tx.customerId == tx.counterpartyId
    val srcId   = if tx.direction == TransactionDirection.Outflow then tx.customerId else tx.counterpartyId
    val tgtId   = if tx.direction == TransactionDirection.Outflow then tx.counterpartyId else tx.customerId
    val edgeKey = if isOwnTransfer then s"OWN:$srcId->$tgtId" else s"$srcId->$tgtId"
    val relType =
      if isOwnTransfer then "OWN_ACCOUNT_TRANSFER"
      else if tx.restrictedTag.isDefined then "RESTRICTED_FLOW"
      else "FINANCIAL_TRANSFER"

    upsertEdge(
      edgeId = edgeKey,
      sourceVertexId = srcId,
      targetVertexId = tgtId,
      relationshipType = relType,
      volumeDelta = tx.amount,
      txCountDelta = 1,
      evidenceIdOpt = Some(tx.txId),
      at = tx.timestamp,
      validFrom = Some(tx.timestamp),
      sourceSystem = "PAYMENT_RAILS",
      lastEventId = Some(tx.txId)
    )

    val customerRisk = tx.restrictedTag.map(_.level.baseRiskWeight * 0.5).getOrElse(0.0)
    upsertVertex(
      vertexId = tx.customerId,
      vertexType = "BANK_CUSTOMER",
      intrinsicRisk = customerRisk,
      at = tx.timestamp
    )

    val cpType =
      if tx.restrictedTag.isDefined then "RESTRICTED_ENTITY"
      else if tx.counterpartyIsBankClient || isOwnTransfer then "BANK_CUSTOMER"
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

    // Section 3.3 & Section 4: Project Account-Level Topology (Customer -> Account -> Transaction -> Account -> Customer)
    val highRiskEntityOpt = tx.restrictedTag.map(_.entityId).orElse(Option.when(tx.category.isHighRiskCategory)(tx.counterpartyId))
    tx.fromAccountId.filter(_.nonEmpty).foreach { fromAcc =>
      val fromOwner = if tx.direction == TransactionDirection.Outflow then tx.customerId else tx.counterpartyId
      upsertVertex(fromAcc, "CUSTOMER_ACCOUNT", customerRisk * 0.5, tx.timestamp)
      upsertEdge(
        edgeId = s"OWNS:$fromOwner->$fromAcc",
        sourceVertexId = fromOwner,
        targetVertexId = fromAcc,
        relationshipType = "CUSTOMER_OWNS_ACCOUNT",
        volumeDelta = BigDecimal(0),
        txCountDelta = 1,
        evidenceIdOpt = Some(tx.txId),
        at = tx.timestamp
      )
      recordAccountObservation(
        accountId = fromAcc,
        ownerCustomerIdOpt = Some(fromOwner),
        linkedCustomerOpt = Some(tgtId),
        txIdOpt = Some(tx.txId),
        amount = tx.amount,
        highRiskEntityOpt = highRiskEntityOpt,
        observedAt = tx.timestamp
      )
    }

    tx.toAccountId.filter(_.nonEmpty).foreach { toAcc =>
      val toOwnerOpt =
        if isOwnTransfer then Some(tx.customerId)
        else if tx.direction == TransactionDirection.Inflow then Some(tx.customerId)
        else if tx.counterpartyIsBankClient then Some(tx.counterpartyId)
        else None

      upsertVertex(toAcc, "CUSTOMER_ACCOUNT", cpRisk, tx.timestamp)
      toOwnerOpt.foreach { toOwner =>
        upsertEdge(
          edgeId = s"OWNS:$toOwner->$toAcc",
          sourceVertexId = toOwner,
          targetVertexId = toAcc,
          relationshipType = "CUSTOMER_OWNS_ACCOUNT",
          volumeDelta = BigDecimal(0),
          txCountDelta = 1,
          evidenceIdOpt = Some(tx.txId),
          at = tx.timestamp
        )
      }
      recordAccountObservation(
        accountId = toAcc,
        ownerCustomerIdOpt = toOwnerOpt,
        linkedCustomerOpt = Some(srcId),
        txIdOpt = Some(tx.txId),
        amount = tx.amount,
        highRiskEntityOpt = highRiskEntityOpt,
        observedAt = tx.timestamp
      )

      tx.fromAccountId.filter(_.nonEmpty).foreach { fromAcc =>
        val accEdgeType = if isOwnTransfer then "OWN_ACCOUNT_TRANSFER" else "FINANCIAL_TRANSFER"
        upsertEdge(
          edgeId = s"ACC_FLOW:$fromAcc->$toAcc",
          sourceVertexId = fromAcc,
          targetVertexId = toAcc,
          relationshipType = accEdgeType,
          volumeDelta = tx.amount,
          txCountDelta = 1,
          evidenceIdOpt = Some(tx.txId),
          at = tx.timestamp,
          lastEventId = Some(tx.txId)
        )
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
   * Native Oracle 23ai/26ai ISO SQL/PGQ `GRAPH_TABLE` query pushing multi-customer hub convergence detection
   * down to Oracle Exadata (`SA_GRAPH.SA_GLOBAL_RELATIONSHIP_GRAPH`, Roadmap 9.6).
   */
  val GlobalGraphPgqSql: String =
    """SELECT shared_entity_id,
      |       COUNT(DISTINCT customer_id) AS linked_customers_count,
      |       LISTAGG(DISTINCT customer_id, ',') WITHIN GROUP (ORDER BY customer_id) AS linked_customers,
      |       MAX(intrinsic_risk) AS max_risk_score,
      |       SUM(total_volume) AS sum_volume,
      |       LISTAGG(evidence_tx_ids, ',') WITHIN GROUP (ORDER BY customer_id) AS combined_evidence
      |FROM GRAPH_TABLE (
      |  SA_GRAPH.SA_GLOBAL_RELATIONSHIP_GRAPH
      |  MATCH (c IS GRAPH_VERTICES WHERE c.VERTEX_TYPE = 'BANK_CUSTOMER')
      |        -[e IS GRAPH_EDGES]->
      |        (hub IS GRAPH_VERTICES)
      |  COLUMNS (
      |    c.VERTEX_ID AS customer_id,
      |    hub.VERTEX_ID AS shared_entity_id,
      |    hub.INTRINSIC_RISK AS intrinsic_risk,
      |    e.TOTAL_VOLUME AS total_volume,
      |    e.EVIDENCE_TX_IDS AS evidence_tx_ids
      |  )
      |)
      |GROUP BY shared_entity_id
      |HAVING COUNT(DISTINCT customer_id) >= 2
      |   AND (MAX(intrinsic_risk) >= 0.50 OR SUM(total_volume) >= 1000)""".stripMargin

  private def analyzeGlobalGraphViaSqlPushdown(conn: Connection, now: Instant): List[NetworkRiskPatternDetected] =
    val results = ListBuffer.empty[NetworkRiskPatternDetected]
    Using.resource(conn.prepareStatement(GlobalGraphPgqSql)) { ps =>
      Using.resource(ps.executeQuery()) { rs =>
        while rs.next() do
          val counterpartyId    = rs.getString("shared_entity_id")
          val linkedCustStr     = Option(rs.getString("linked_customers")).getOrElse("")
          val distinctCustomers = linkedCustStr.split(",").map(_.trim).filter(_.nonEmpty).distinct.toList
          val rawScore          = rs.getDouble("max_risk_score")
          val baseScore         = if rawScore > 0.0 then rawScore else 0.55
          val patternScore      = BigDecimal(math.min(0.95, baseScore)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
          val evRaw             = Option(rs.getString("combined_evidence")).getOrElse("")
          val allEvidence       = evRaw.split(",").map(_.trim).filter(_.nonEmpty).distinct.toList

          distinctCustomers.foreach { affectedCust =>
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
                s"Global Property Graph (SQL/PGQ GRAPH_TABLE) detected ${distinctCustomers.size} bank accounts (${distinctCustomers.mkString(", ")}) converging on shared counterparty '$counterpartyId'",
              evidenceIds = allEvidence,
              detectedAt = now
            )
            results += pattern
          }
      }
    }
    results.toList

  /**
   * Executes Global Relationship Graph Analysis on Oracle Exadata (`SA_GRAPH` -> `SA_INTELLIGENCE`, Section 5, 14 & Roadmap 9.6).
   * When connected to Oracle 23ai/26ai JDBC, attempts native `GRAPH_TABLE` SQL/PGQ pushdown first,
   * and merges with in-memory graph analysis.
   */
  def analyzeGlobalGraphAndDetectPatterns(now: Instant = Instant.now()): List[NetworkRiskPatternDetected] =
    val pgqDetected: List[NetworkRiskPatternDetected] =
      withJdbcConnection(conn => analyzeGlobalGraphViaSqlPushdown(conn, now)).getOrElse(Nil)

    val allEdges    = graphEdges.values().asScala.toList
    val allVertices = graphVertices.asScala.toMap
    val transferEdges = allEdges.filter(e =>
      (e.relationshipType == "FINANCIAL_TRANSFER" || e.relationshipType == "RESTRICTED_FLOW") &&
      !e.edgeId.startsWith("ACC_FLOW:") &&
      e.sourceVertexId != e.targetVertexId
    )

    val byCounterparty: Map[String, List[GraphEdge]] = transferEdges.groupBy(_.targetVertexId)

    // 1. Existing Shared High-Risk Counterparty & High-Degree Hub detector
    val inMemPatterns = byCounterparty.toList.flatMap { case (counterpartyId, incomingEdges) =>
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
          NetworkRiskPatternDetected(
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
        }
      else Nil
    }

    // 2. Employee -> Phone Number -> Customer serviced by that employee (Doc/NowePotrzeby.md Section 2)
    val empPhoneEdges  = allEdges.filter(_.relationshipType == "EMPLOYEE_HAS_PHONE")
    val custPhoneEdges = allEdges.filter(e => e.relationshipType == "CUSTOMER_HAS_PHONE" && e.validTo.isEmpty)
    val empServEdges   = allEdges.filter(_.relationshipType == "EMPLOYEE_SERVICES_CUSTOMER")

    val employeeCustomerPatterns: List[NetworkRiskPatternDetected] =
      for
        empEdge  <- empPhoneEdges
        custEdge <- custPhoneEdges if custEdge.targetVertexId == empEdge.targetVertexId
      yield
        val empId      = empEdge.sourceVertexId
        val custId     = custEdge.sourceVertexId
        val phoneVert  = empEdge.targetVertexId
        val isServiced = empServEdges.exists(s => s.sourceVertexId == empId && s.targetVertexId == custId)
        val score      = if isServiced then 0.68 else 0.50
        NetworkRiskPatternDetected(
          patternId = s"PAT-EMP-PHONE-${empId.replaceAll("[^A-Za-z0-9]", "")}-${custId.replaceAll("[^A-Za-z0-9]", "")}",
          patternType = NetworkRiskPatternType.EmployeeCustomerPhoneMatch,
          affectedCustomerId = custId,
          sharedEntityId = phoneVert,
          linkedCustomers = List(empId, custId),
          riskScore = score,
          reasonCode = "EMPLOYEE_CUSTOMER_PHONE_MATCH",
          description =
            s"Wykryto powiązanie w grafie SA_GRAPH: Pracownik '$empId' -> $phoneVert -> Klient '$custId' (obsługiwany przez tego pracownika: ${if isServiced then "TAK" else "NIE"})",
          evidenceIds = (empEdge.evidenceTxIds ++ custEdge.evidenceTxIds).distinct,
          detectedAt = now
        )

    // 3. Mule Detector A: Rapid Pass-Through (Doc/NowePotrzeby.md Section 3.2: Klient A -> Klient B (Słup) -> Klient C)
    val incomingByNode = transferEdges.groupBy(_.targetVertexId)
    val outgoingByNode = transferEdges.groupBy(_.sourceVertexId)

    val rapidPassThroughPatterns: List[NetworkRiskPatternDetected] =
      incomingByNode.toList.flatMap { case (intermediaryId, inEdges) =>
        val outEdges = outgoingByNode.getOrElse(intermediaryId, Nil).filter(_.targetVertexId != intermediaryId)
        val totalIn  = inEdges.map(_.totalVolume).sum
        val totalOut = outEdges.map(_.totalVolume).sum
        if totalIn >= BigDecimal("5000.00") && outEdges.nonEmpty then
          val passThroughRatio = (totalOut / totalIn).setScale(4, BigDecimal.RoundingMode.HALF_UP)
          if passThroughRatio >= BigDecimal("0.75") && passThroughRatio <= BigDecimal("1.05") then
            val senders   = inEdges.map(_.sourceVertexId).distinct
            val receivers = outEdges.map(_.targetVertexId).filterNot(senders.contains).distinct
            if receivers.nonEmpty then
              val evidence = (inEdges.flatMap(_.evidenceTxIds) ++ outEdges.flatMap(_.evidenceTxIds)).distinct
              val chainParticipants = (senders ++ List(intermediaryId) ++ receivers).distinct
              val affectedBankCustomers = chainParticipants.filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER"))
              affectedBankCustomers.map { affectedCust =>
                NetworkRiskPatternDetected(
                  patternId = s"PAT-MULE-PASSTHROUGH-${intermediaryId.replaceAll("[^A-Za-z0-9]", "")}-${affectedCust.replaceAll("[^A-Za-z0-9]", "")}",
                  patternType = NetworkRiskPatternType.RapidPassThrough,
                  affectedCustomerId = affectedCust,
                  sharedEntityId = intermediaryId,
                  linkedCustomers = chainParticipants,
                  riskScore = 0.85,
                  reasonCode = "SUSPECTED_MULE_ACCOUNT",
                  description =
                    s"Rapid Pass-Through (Konto Słup '$intermediaryId'): otrzymano $totalIn od [${senders.mkString(", ")}] i natychmiast przekazano $totalOut (${(passThroughRatio * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP)}%) dalej do [${receivers.mkString(", ")}]",
                  evidenceIds = evidence,
                  detectedAt = now
                )
              }
            else Nil
          else Nil
        else Nil
      }

    // 4. Mule Detector B: Fan-In / Fan-Out (Doc/NowePotrzeby.md Section 3.2)
    val fanInFanOutPatterns: List[NetworkRiskPatternDetected] =
      incomingByNode.toList.flatMap { case (hubId, inEdges) =>
        val outEdges = outgoingByNode.getOrElse(hubId, Nil)
        val distinctSenders   = inEdges.map(_.sourceVertexId).distinct
        val distinctReceivers = outEdges.map(_.targetVertexId).distinct
        if (distinctSenders.size >= 2 && distinctReceivers.nonEmpty) || (distinctSenders.nonEmpty && distinctReceivers.size >= 2) then
          val totalVol = inEdges.map(_.totalVolume).sum + outEdges.map(_.totalVolume).sum
          if totalVol >= BigDecimal("3000.00") then
            val participants = (distinctSenders ++ List(hubId) ++ distinctReceivers).distinct
            val evidence     = (inEdges.flatMap(_.evidenceTxIds) ++ outEdges.flatMap(_.evidenceTxIds)).distinct
            participants
              .filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER"))
              .map { affectedCust =>
                NetworkRiskPatternDetected(
                  patternId = s"PAT-MULE-FAN-${hubId.replaceAll("[^A-Za-z0-9]", "")}-${affectedCust.replaceAll("[^A-Za-z0-9]", "")}",
                  patternType = NetworkRiskPatternType.FanInFanOut,
                  affectedCustomerId = affectedCust,
                  sharedEntityId = hubId,
                  linkedCustomers = participants,
                  riskScore = 0.78,
                  reasonCode = "SUSPECTED_MULE_ACCOUNT",
                  description =
                    s"Fan-In / Fan-Out wokół '$hubId' (nadawcy: ${distinctSenders.size} [${distinctSenders.mkString(", ")}] -> '$hubId' -> odbiorcy: ${distinctReceivers.size} [${distinctReceivers.mkString(", ")}])",
                  evidenceIds = evidence,
                  detectedAt = now
                )
              }
          else Nil
        else Nil
      }

    // 5. Mule Detector C & D: Multi-Hop Money Movement (A -> B -> C -> D) & Circular Transfers (A -> B -> C -> A or A -> B -> A)
    val multiHopAndCircularPatterns = ListBuffer.empty[NetworkRiskPatternDetected]
    for
      e1 <- transferEdges
      a  = e1.sourceVertexId
      b  = e1.targetVertexId if a != b
      e2 <- outgoingByNode.getOrElse(b, Nil)
      c  = e2.targetVertexId if c != b
    do
      if c == a then
        // 2-cycle Circular Transfer: A -> B -> A
        val chain    = List(a, b, a)
        val evidence = (e1.evidenceTxIds ++ e2.evidenceTxIds).distinct
        List(a, b).filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER")).foreach { affected =>
          multiHopAndCircularPatterns += NetworkRiskPatternDetected(
            patternId = s"PAT-CIRCULAR-${a.replaceAll("[^A-Za-z0-9]", "")}-${b.replaceAll("[^A-Za-z0-9]", "")}-${affected.replaceAll("[^A-Za-z0-9]", "")}",
            patternType = NetworkRiskPatternType.CircularTransfers,
            affectedCustomerId = affected,
            sharedEntityId = b,
            linkedCustomers = chain.distinct,
            riskScore = 0.80,
            reasonCode = "SUSPECTED_MULE_ACCOUNT",
            description = s"Circular Transfers (Karuzela środków): wykryto zamknięty obieg ${chain.mkString(" -> ")}",
            evidenceIds = evidence,
            detectedAt = now
          )
        }
      else
        for
          e3 <- outgoingByNode.getOrElse(c, Nil)
          d  = e3.targetVertexId if d != c && d != b
        do
          val evidence = (e1.evidenceTxIds ++ e2.evidenceTxIds ++ e3.evidenceTxIds).distinct
          if d == a then
            // 3-cycle Circular Transfer: A -> B -> C -> A
            val chain = List(a, b, c, a)
            List(a, b, c).filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER")).foreach { affected =>
              multiHopAndCircularPatterns += NetworkRiskPatternDetected(
                patternId = s"PAT-CIRCULAR3-${a.replaceAll("[^A-Za-z0-9]", "")}-${b.replaceAll("[^A-Za-z0-9]", "")}-${affected.replaceAll("[^A-Za-z0-9]", "")}",
                patternType = NetworkRiskPatternType.CircularTransfers,
                affectedCustomerId = affected,
                sharedEntityId = b,
                linkedCustomers = List(a, b, c),
                riskScore = 0.86,
                reasonCode = "SUSPECTED_MULE_ACCOUNT",
                description = s"Circular Transfers (3-hop): środki wróciły do uczestnika początkowego (${chain.mkString(" -> ")})",
                evidenceIds = evidence,
                detectedAt = now
              )
            }
          else
            // 3-hop Multi-Hop Money Movement chain: A -> B -> C -> D (Doc/NowePotrzeby.md Section 3.2)
            val chain = List(a, b, c, d)
            chain.filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER")).foreach { affected =>
              multiHopAndCircularPatterns += NetworkRiskPatternDetected(
                patternId = s"PAT-MULTIHOP-${b.replaceAll("[^A-Za-z0-9]", "")}-${c.replaceAll("[^A-Za-z0-9]", "")}-${affected.replaceAll("[^A-Za-z0-9]", "")}",
                patternType = NetworkRiskPatternType.MultiHopMoneyMovement,
                affectedCustomerId = affected,
                sharedEntityId = b,
                linkedCustomers = chain,
                riskScore = 0.88,
                reasonCode = "SUSPECTED_MULE_ACCOUNT",
                description =
                  s"Multi-Hop Money Movement: łańcuch transferów ${chain.mkString(" -> ")} (wypływ z '$a' przez pośredników '$b' i '$c' do '$d')",
                evidenceIds = evidence,
                detectedAt = now
              )
            }

    val combined = (
      pgqDetected ++
      inMemPatterns ++
      employeeCustomerPatterns ++
      rapidPassThroughPatterns ++
      fanInFanOutPatterns ++
      multiHopAndCircularPatterns.toList
    ).distinctBy(p => (p.patternId, p.affectedCustomerId))

    combined.foreach(recordRiskIntelligenceEvent)
    combined

  /**
   * Interactive Graph Investigation Workbench query (`Doc/NowePotrzeby.md` Section 5):
   * Allows an analyst to start from any entity (`rootEntityId`), expand relationships up to `depth` hops,
   * filter by relationship type, and inspect shared phones, shared accounts, shared counterparties, and risk provenance.
   */
  def investigateGraph(
      rootEntityId: String,
      depth: Int = 2,
      relationshipTypeFilter: Option[String] = None,
      now: Instant = Instant.now()
  ): GraphInvestigationResult =
    val boundedDepth = math.max(1, math.min(4, depth))
    val allEdges     = graphEdges.values().asScala.toList
    val allVertices  = graphVertices.asScala.toMap

    val filteredAllEdges = relationshipTypeFilter.map(_.trim).filter(_.nonEmpty) match
      case Some(relFilter) => allEdges.filter(_.relationshipType.equalsIgnoreCase(relFilter))
      case None            => allEdges

    var visitedVertices = Set(rootEntityId)
    var collectedEdges  = Map.empty[String, GraphEdge]

    for _ <- 1 to boundedDepth do
      val hopEdges = filteredAllEdges.filter(e =>
        visitedVertices.contains(e.sourceVertexId) || visitedVertices.contains(e.targetVertexId)
      )
      hopEdges.foreach { e =>
        collectedEdges = collectedEdges.updated(e.edgeId, e)
        visitedVertices = visitedVertices + e.sourceVertexId + e.targetVertexId
      }

    val subVertices = visitedVertices.toList.flatMap(vid => allVertices.get(vid)).sortBy(_.vertexId)
    val subEdges    = collectedEdges.values.toList.sortBy(_.edgeId)

    val sharedPhones = subVertices
      .filter(_.vertexType == "PHONE_NUMBER")
      .filter { pv =>
        val connectedOwners = allEdges
          .filter(e => e.targetVertexId == pv.vertexId && (e.relationshipType == "CUSTOMER_HAS_PHONE" || e.relationshipType == "EMPLOYEE_HAS_PHONE"))
          .map(_.sourceVertexId)
          .distinct
        connectedOwners.size >= 2
      }
      .map(_.vertexId)

    val sharedAccounts = subVertices
      .filter(_.vertexType == "CUSTOMER_ACCOUNT")
      .filter { av =>
        val connected = allEdges
          .filter(e => e.sourceVertexId == av.vertexId || e.targetVertexId == av.vertexId)
          .flatMap(e => List(e.sourceVertexId, e.targetVertexId))
          .filter(_ != av.vertexId)
          .distinct
        connected.size >= 2
      }
      .map(_.vertexId)

    val sharedCounterparties = subVertices
      .filter(v => v.vertexType == "EXTERNAL_ENTITY" || v.vertexType == "RESTRICTED_ENTITY")
      .filter { cp =>
        val connectedCusts = allEdges
          .filter(e => e.targetVertexId == cp.vertexId || e.sourceVertexId == cp.vertexId)
          .flatMap(e => List(e.sourceVertexId, e.targetVertexId))
          .filter(id => allVertices.get(id).exists(_.vertexType == "BANK_CUSTOMER"))
          .distinct
        connectedCusts.size >= 2
      }
      .map(_.vertexId)

    val empLinks = contactChangeHistory.asScala.toList.filter(c =>
      visitedVertices.contains(c.customerId) || c.matchedEmployeeId.exists(visitedVertices.contains)
    )

    val relPatterns = riskIntelEvents.asScala.toList.filter(p =>
      visitedVertices.contains(p.affectedCustomerId) ||
      visitedVertices.contains(p.sharedEntityId) ||
      p.linkedCustomers.exists(visitedVertices.contains)
    )

    GraphInvestigationResult(
      rootEntityId = rootEntityId,
      depth = boundedDepth,
      relationshipTypeFilter = relationshipTypeFilter,
      vertices = subVertices,
      edges = subEdges,
      sharedPhones = sharedPhones,
      sharedAccounts = sharedAccounts,
      sharedCounterparties = sharedCounterparties,
      employeeCustomerLinks = empLinks,
      relatedPatterns = relPatterns,
      queriedAt = now
    )

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

  def recordTransactionAuthorizationAudit(response: TransactionAuthorizationResponse): Unit =
    txAuthorizationAuditLog.addFirst(response)
    while txAuthorizationAuditLog.size() > 200 do txAuthorizationAuditLog.removeLast()

    withJdbcConnection { conn =>
      val reasons = response.principalReasons.map(_.code).mkString(",")
      val sql =
        """MERGE INTO SA_DECISIONS.TRANSACTION_AUTHORIZATION_LOG t
          |USING (SELECT ? AS tid FROM dual) s ON (t.TRANSFER_ID = s.tid)
          |WHEN NOT MATCHED THEN INSERT
          |  (TRANSFER_ID, CUSTOMER_ID, TO_ACCOUNT_ID, TO_COUNTERPARTY_ID, AMOUNT, DECISION_STATUS, RISK_SCORE, REASON_CODES, RAIL_DIRECTIVE, EVALUATED_AT)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { ps =>
        ps.setString(1, response.transferId)
        ps.setString(2, response.transferId)
        ps.setString(3, response.customerId)
        ps.setString(4, response.toAccountId)
        ps.setString(5, response.toCounterpartyId)
        ps.setBigDecimal(6, response.amount.bigDecimal)
        ps.setString(7, response.decision.toString)
        ps.setInt(8, response.riskScore)
        ps.setString(9, if reasons.isEmpty then "NONE" else reasons)
        ps.setString(10, response.paymentRailDirective)
        ps.setTimestamp(11, Timestamp.from(response.evaluatedAt))
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

  def getCustomerAccounts: List[CustomerAccount] =
    customerAccounts.values().asScala.toList

  def getAccountHistoryRegistry: List[AccountVerificationResult] =
    accountHistoryMap.values().asScala.toList

  def getAccountFirstSeenRegistry: List[AccountFirstSeen] =
    accountFirstSeenMap.values().asScala.toList

  def getEmployees: List[EmployeeProfile] =
    employeeRegistry.values().asScala.toList

  def getContactChangeHistory: List[NormalizedCustomerContactChange] =
    contactChangeHistory.asScala.toList

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

  def getTransactionAuthorizationAuditLog: List[TransactionAuthorizationResponse] =
    txAuthorizationAuditLog.asScala.toList

  def getGovernancePolicies: List[GovernancePolicyRow] =
    governancePolicies.values().asScala.toList

  def getSchemaRecordCounts: Map[String, Int] = Map(
    "SA_PERSISTENCE"  -> (persistenceJournal.size() + persistenceSnapshots.size() + persistenceOutbox.size()),
    "SA_FACTS"        -> (financialTxFacts.size() + verifiedDocFacts.size() + debtObligationFacts.size() + accountHistoryMap.size() + contactChangeHistory.size()),
    "SA_GRAPH"        -> (graphVertices.size() + graphEdges.size()),
    "SA_INTELLIGENCE" -> (riskIntelEvents.size() + portfolioSnapshots.size()),
    "SA_DECISIONS"    -> (decisionAuditLog.size() + projectionAuditLog.size() + txAuthorizationAuditLog.size()),
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
    customerAccounts.clear()
    accountFirstSeenMap.clear()
    accountHistoryMap.clear()
    employeeRegistry.clear()
    contactChangeHistory.clear()
    graphVertices.clear()
    graphEdges.clear()
    riskIntelEvents.clear()
    portfolioSnapshots.clear()
    decisionAuditLog.clear()
    projectionAuditLog.clear()
    txAuthorizationAuditLog.clear()
