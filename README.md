# Real-Time Banking Situation Awareness & Explainable Credit Decisioning

A **Scala 3** reference implementation of Endsley’s **Situation Awareness (SA)** model for real-time consumer loan decisioning and regulatory explainability (**EU AI Act / GDPR Art. 22 / US ECOA Adverse Action Notices**), built on **Apache Kafka**, **Apache Flink**, and **Apache Pekko Cluster Sharding**.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the complete C4 architecture diagrams, Kafka topic topology, Protobuf schemas, 2-hop risk propagation protocol, and memory sizing calculations.

---

## Module Structure

| Module | Path | Description |
| :--- | :--- | :--- |
| `sa-domain` | `modules/domain` | Core SA domain model, incremental `SlidingWindowCashflow` hourly ring buffer ($O(1)$ updates replacing SQL window queries), `CustomerSituationState`, and regulatory `ReasonCode` / `ExplainableAuditTrail` models. |
| `sa-explainable-engine` | `modules/explainable-engine` | Pure functional `ExplainableCreditEngine` combining deterministic policy knockouts and an additive credit scorecard (`300–850`) with exact evidence IDs and counterfactual resolution guidance. |
| `sa-enrichment-pipeline` | `modules/enrichment-pipeline` | Apache Flink streaming pipeline (`BilateralTransactionSplitter`, `WatchlistBroadcastEnricher`, and `FlinkKafkaEnrichmentJob`) for 1st-hop watchlist tagging and customer-keyed event routing. |
| `sa-customer-agents` | `modules/customer-agents` | Event-sourced `CustomerAgent` Digital Twin actors on Pekko Cluster Sharding, automatic 2nd-hop actor-to-actor `PeerRiskSignal` propagation, Pekko Streams ingestion (`SituationStreamProcessor`), Pekko HTTP/gRPC server (`LoanOriginationServer`), and the Interactive Business Workbench UI. |

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
Then open **`http://127.0.0.1:8080/`** in your browser to:
- Step through the **4-Stage Guided Demo Scenario** (or click **Run Full Guided Demo**).
- Click any `CustomerAgent` shard card to inspect its live 30-day cashflow ring buffer, verified documents, ego-network peers, and 1st/2nd-hop risk exposures.
- Submit custom **Loan Applications**, **Transactions**, and **Verified Documents** in the interactive playground.
- Inspect the live **Flink Enrichment & Pekko Actor Message Bus Log** (including `AGENT_TO_AGENT_IPC` 2nd-hop risk signals).

### 3. Run the CLI Simulation Demo
```bash
sbt "project customerAgents" "runMain com.bank.sa.app.SituationAwarenessDemo"
```

---

## REST & gRPC HTTP API Examples

With `LoanOriginationWebApp` running on `127.0.0.1:8080`:

```bash
# 1. Trigger Guided Demo Step 1 (Hydrate Alice & Bob profiles)
curl -X POST http://127.0.0.1:8080/api/v1/demo/scenario/step1-bootstrap

# 2. Evaluate a Consumer Loan Request (REST or gRPC path)
curl -X POST http://127.0.0.1:8080/api/v1/loans/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "applicationId": "LOAN-REQ-900",
    "customerId": "CUST-ALICE-001",
    "requestedAmount": 12000.00,
    "termMonths": 24,
    "annualInterestRate": 0.065
  }'

# 3. Inspect a Customer's Live Situation Awareness State
curl http://127.0.0.1:8080/api/v1/customers/CUST-ALICE-001/situation

# 4. Inspect Full Cluster Overview & Inter-Agent Workflow Telemetry
curl http://127.0.0.1:8080/api/v1/system/overview
```

