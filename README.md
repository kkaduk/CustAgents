# Real-Time Banking Situation Awareness & Explainable Credit Decisioning (Apache Pekko + Oracle Exadata)

A **Scala 3** reference implementation of Endsley’s **Situation Awareness (SA)** model for real-time consumer loan decisioning and regulatory explainability (**EU AI Act / GDPR Art. 22 / US ECOA Adverse Action Notices**), built on **Apache Kafka**, **Apache Flink** (with an **External Transaction Categorizer Module**), **Apache Pekko Cluster Sharding**, and **Oracle Exadata** (`SA_PERSISTENCE`, `SA_FACTS`, `SA_GRAPH`, `SA_INTELLIGENCE`, `SA_DECISIONS`, `SA_GOVERNANCE`).

See [Doc/ExaPekdoDesign.md](Doc/ExaPekdoDesign.md) and [ARCHITECTURE.md](ARCHITECTURE.md) for the complete architectural design, Oracle Exadata 6-schema separation, Property Graph risk intelligence cycle, Event Plugin architecture, and 5-Agent hierarchical model.

---

## Core Architectural Pillars (`Doc/ExaPekdoDesign.md`)

1. **Five Sharded Domain Agent Types (`Apache Pekko Cluster Sharding`):**
   - `CustomerAgent`: Maintains real-time situation of a single customer (`CustomerSituationState`), routing observed events (`TransactionObserved`, `IncomeVerified`, `DebtUpdated`, `CounterpartyRiskReceived`, `ExternalDocumentReceived`, `NetworkRiskPatternDetected`) through the configurable `EventPluginRegistry` and emitting `SituationEvaluated`.
   - `RelationshipAgent`: Tracks bilateral relationship and exposure state (`RelationshipSituationState`).
   - `RiskSignalAgent`: Coordinates interpretation, deduplication, attenuation, and reliable delivery lifecycle of risk signals (`RiskSignalAgentState`).
   - `DocumentVerificationAgent`: Coordinates document verification and classifies facts into observed domain events (`IncomeVerified`, `DebtUpdated`, `ExternalDocumentReceived`).
   - `PortfolioRiskAgent`: Interprets risk concentration at the segment/portfolio level (`PortfolioSituationState`).
2. **Oracle Exadata as Global Memory & Intelligence Platform (6 Schemas in [sql/oracle_exadata_schemas.sql](sql/oracle_exadata_schemas.sql)):**
   - `SA_PERSISTENCE`: Pekko Persistence JDBC Event Journal (`Slick OracleProfile`), bounded state snapshots, durable state, and Transactional Outbox (`AGENT_TRANSACTIONAL_OUTBOX`).
   - `SA_FACTS`: Authoritative canonical history of financial transactions, verified documents, and debt obligations.
   - `SA_GRAPH`: Oracle Property Graph (`GRAPH_VERTICES`, `GRAPH_EDGES`) detecting multi-customer shared counterparties and high-degree risk hubs outside the single-decision critical path.
   - `SA_INTELLIGENCE`: Global risk intelligence (`NetworkRiskPatternDetected`) and portfolio analytics snapshots.
   - `SA_DECISIONS`: Immutable audit logs of Level-3 `SituationProjection` scenarios and formal `LoanDecisionResponse` evaluations.
   - `SA_GOVERNANCE`: Versioned credit policies, scoring models, and data completeness thresholds.
3. **Reliability Guarantees:**
   - **Idempotency:** Bounded recent IDs + compact historical event digest (`processedEventDigests`) preventing duplicate mutation even for historical events.
   - **Transactional Outbox & Reliable Delivery:** Outgoing inter-agent signals are persisted atomically in `pendingOutbox` and cleared upon delivery acknowledgment (`ConfirmOutboxDelivery`).
   - **Situation Consistency:** Every projection and decision records `stateVersion`, `policyVersion`, and `DataCompleteness` (missing verified facts are never treated as absence of risk).

---

## Module Structure

| Module | Path | Description |
| :--- | :--- | :--- |
| `sa-domain` | `modules/domain` | Core SA domain model, `EventPluginRegistry` & `SituationEventPlugin` implementations, `SlidingWindowCashflow`, `CustomerSituationState`, hierarchical agent states, `SituationProjection`, and regulatory `ReasonCode` / `ExplainableAuditTrail` models. |
| `sa-explainable-engine` | `modules/explainable-engine` | Pure functional `ExplainableCreditEngine` providing both Level-3 `projectSituation` (future DTI, expected liquidity, affordability) and formal `evaluate` credit decisions (`300–850`). |
| `sa-enrichment-pipeline` | `modules/enrichment-pipeline` | Apache Flink streaming pipeline (`TransactionValidationFilter`, `ExternalTransactionCategorizer`, `BilateralTransactionSplitter`, `WatchlistBroadcastEnricher`, and `FlinkKafkaEnrichmentJob`). |
| `sa-customer-agents` | `modules/customer-agents` | All 5 sharded Pekko Agent types (`CustomerAgent`, `RelationshipAgent`, `RiskSignalAgent`, `DocumentVerificationAgent`, `PortfolioRiskAgent`), `OracleExadataRepository`, Pekko Streams ingestion, Pekko HTTP/gRPC server (`LoanOriginationServer`), and Interactive Business Workbench UI. |

---

## Quick Start

### 1. Run the Test Suite
```bash
sbt test
```

### 2. Launch the Interactive Web Workbench & Pekko HTTP/gRPC Server
```bash
sbt "project customerAgents" "runMain com.bank.sa.app.LoanOriginationWebApp"
```
Then open **`http://127.0.0.1:8088/`** in your browser.

### 3. Run the CLI Simulation Demo
```bash
sbt "project customerAgents" "runMain com.bank.sa.app.SituationAwarenessDemo"
```

---

## REST & gRPC HTTP API Examples

With `LoanOriginationWebApp` running on `127.0.0.1:8088`:

```bash
# 1. Trigger Guided Demo Step 1 (Hydrate Alice & Bob profiles)
curl -X POST http://127.0.0.1:8088/api/v1/demo/scenario/step1-bootstrap

# 2. Compute a Level-3 Situation Projection (independent of formal approval)
curl -X POST http://127.0.0.1:8088/api/v1/loans/project \
  -H "Content-Type: application/json" \
  -d '{
    "applicationId": "PROJ-REQ-900",
    "customerId": "CUST-ALICE-001",
    "requestedAmount": 12000.00,
    "termMonths": 24,
    "annualInterestRate": 0.065
  }'

# 3. Evaluate a Formal Consumer Loan Request
curl -X POST http://127.0.0.1:8088/api/v1/loans/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "applicationId": "LOAN-REQ-900",
    "customerId": "CUST-ALICE-001",
    "requestedAmount": 12000.00,
    "termMonths": 24,
    "annualInterestRate": 0.065
  }'

# 4. Run Oracle Exadata Global Property Graph Analysis (emits NetworkRiskPatternDetected)
curl -X POST http://127.0.0.1:8088/api/v1/oracle/graph/analyze

# 5. Inspect Oracle Exadata 6-Schema Overview & Hierarchical Agent States
curl http://127.0.0.1:8088/api/v1/oracle/overview
curl http://127.0.0.1:8088/api/v1/agents/hierarchy
```
