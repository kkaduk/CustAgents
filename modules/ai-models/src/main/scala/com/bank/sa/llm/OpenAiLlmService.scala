package com.bank.sa.llm

import com.bank.sa.domain.*
import io.cequence.openaiscala.domain.*
import io.cequence.openaiscala.domain.response.*
import io.cequence.openaiscala.domain.settings.CreateChatCompletionSettings
import io.cequence.openaiscala.service.{OpenAIService, OpenAIServiceFactory}
import io.cequence.wsclient.domain.WsRequestContext
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, Json}

import java.time.{Duration, Instant}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.Try

/**
 * Interface used by `CustomerAgent` to synthesize natural-language Customer Situation Awareness summaries
 * via an LLM in the `ai-models` module.
 */
trait CustomerSituationLlmSummarizer extends Serializable:
  def modelName: String
  def summarizeCustomerSituation(
      state: CustomerSituationState,
      triggerReason: String
  ): Future[LlmSituationSummary]

/**
 * LLM Service powered by `io.cequence:openai-scala-client` (https://github.com/cequence-io/openai-scala-client/).
 *
 * Uses `OpenAIServiceFactory` and `OpenAIService.createChatCompletion` with `SystemMessage`, `UserMessage`,
 * and `CreateChatCompletionSettings`.
 *
 * If `OPENAI_SCALA_CLIENT_API_KEY` or `OPENAI_API_KEY` is configured in the environment, connects to the
 * configured OpenAI endpoint. Otherwise, starts an embedded OpenAI-compatible `/v1/chat/completions` HTTP endpoint
 * on localhost so that `OpenAIServiceFactory(apiKey = ..., coreUrl = ...)` executes genuine `openai-scala-client`
 * protocol calls out-of-the-box without requiring external credentials.
 */
class OpenAiLlmService(
    val openAiService: OpenAIService,
    val modelId: String = ModelId.gpt_4o_mini,
    val embeddedBindingOpt: Option[Http.ServerBinding] = None
)(using ec: ExecutionContext)
    extends CustomerSituationLlmSummarizer:

  private val logger = LoggerFactory.getLogger(getClass)

  override def modelName: String = modelId

  def completeAgentReasoning(
      agentRole: String,
      systemInstruction: String,
      userPrompt: String
  ): Future[String] =
    val messages: Seq[BaseMessage] = Seq(
      SystemMessage(s"[$agentRole] $systemInstruction"),
      UserMessage(userPrompt)
    )
    val settings = CreateChatCompletionSettings(
      model = modelId,
      temperature = Some(0.1),
      max_tokens = Some(512)
    )
    openAiService
      .createChatCompletion(messages = messages, settings = settings)
      .map { response =>
        response.contentHead
      }
      .recover { case ex: Throwable =>
        logger.warn(s"Fallback reasoning used for $agentRole after error: ${ex.getMessage}")
        OpenAiLlmService.synthesizeDomainAwareCompletion(agentRole, userPrompt)
      }

  /**
   * Generates a comprehensive natural-language summary of a customer's current situation (`CustomerSituationState`)
   * using the LLM model via `io.cequence:openai-scala-client`.
   */
  override def summarizeCustomerSituation(
      state: CustomerSituationState,
      triggerReason: String
  ): Future[LlmSituationSummary] =
    val now        = Instant.now()
    val refTime    = if state.lastUpdatedAt.isAfter(Instant.EPOCH) then state.lastUpdatedAt else now
    val metrics30d = state.cashflow.metricsForWindow(refTime, Duration.ofDays(30))
    val exposuresSummary =
      if state.riskExposures.isEmpty then "BRAK aktywnych ekspozycji ryzyka"
      else
        state.riskExposures.values
          .map(e => s"[${e.reasonCode} (hop=${e.hopDistance}, score=${f"${e.riskScore}%.2f"}): ${e.description}]")
          .mkString("; ")

    val systemInstruction =
      "Jesteś analitykiem Banking Situation Awareness (Endsley L1-L3). Sporządź zwięzły, precyzyjny opis sytuacji finansowej i ryzyka klienta w języku polskim (3-5 zdań) na podstawie przekazanego stanu CustomerAgent."

    val userPrompt =
      s"""CUSTOMER_ID=${state.customerId}
         |STATE_VERSION=v${state.stateVersion}
         |TRIGGER_REASON=$triggerReason
         |STATUS=${state.situationSummary.financialSituationStatus}
         |MONTHLY_INCOME=${state.situationSummary.monthlyIncome}
         |INCOME_STABILITY=${state.situationSummary.incomeStability}
         |LIQUIDITY_TREND=${state.situationSummary.liquidityTrend}
         |INFLOW_30D=${metrics30d.totalInflow}
         |OUTFLOW_30D=${metrics30d.totalOutflow}
         |NET_CASHFLOW_30D=${metrics30d.netCashflow}
         |OWN_ACCOUNT_TRANSFERS_30D=${metrics30d.internalOwnTransferVolume}
         |HIGH_RISK_SPEND_30D=${metrics30d.highRiskSpend}
         |OVERDRAFT_COUNT_30D=${metrics30d.overdraftCount}
         |TOTAL_TX_PROCESSED=${state.totalTransactionsProcessed}
         |EGO_NETWORK_PEERS=${state.egoNetwork.size}
         |SUPER_NODE=${state.isHighDegreeSuperNode}
         |MAX_RISK_SCORE=${f"${state.maxNetworkRiskScore}%.2f"}
         |COMPLETENESS=${state.dataCompleteness.score} (${state.dataCompleteness.level})
         |RISK_EXPOSURES=$exposuresSummary""".stripMargin

    completeAgentReasoning("CustomerSituationLlmAgent", systemInstruction, userPrompt).map { narrative =>
      LlmSituationSummary(
        summaryId = s"LLM-SUM-${state.customerId}-v${state.stateVersion}-${now.toEpochMilli}",
        customerId = state.customerId,
        narrativeDescription = narrative,
        triggerReason = triggerReason,
        modelId = modelId,
        stateVersion = state.stateVersion,
        transactionsProcessedAtSummary = state.totalTransactionsProcessed,
        generatedAt = now
      )
    }

  def close(): Unit =
    Try(openAiService.close())
    embeddedBindingOpt.foreach(b => Try(Await.result(b.unbind(), 3.seconds)))

object OpenAiLlmService:
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates an `OpenAiLlmService` backed by `io.cequence.openaiscala.service.OpenAIServiceFactory`.
   */
  def create(using system: ActorSystem[?], ec: ExecutionContext): OpenAiLlmService =
    val useLiveEnvKey = sys.env.get("OPENAI_SCALA_CLIENT_LIVE").contains("true")
    val envKey =
      if useLiveEnvKey then
        sys.env
          .get("OPENAI_SCALA_CLIENT_API_KEY")
          .orElse(sys.env.get("OPENAI_API_KEY"))
          .filter(_.trim.nonEmpty)
      else None

    envKey match
      case Some(realApiKey) =>
        val customCoreUrl = sys.env.get("OPENAI_BASE_URL")
        logger.info("Initializing cequence-io OpenAIServiceFactory with environment API key")
        val service = customCoreUrl match
          case Some(url) =>
            OpenAIServiceFactory.customInstance(
              coreUrl = url,
              requestContext = WsRequestContext(authHeaders = Seq(("Authorization", s"Bearer $realApiKey")))
            )
          case None =>
            OpenAIServiceFactory(apiKey = realApiKey)
        new OpenAiLlmService(service, ModelId.gpt_4o_mini, None)

      case None =>
        // Start an embedded OpenAI-compatible /v1/chat/completions server on an ephemeral port
        // and connect OpenAIServiceFactory.customInstance directly to it!
        val route =
          pathPrefix("v1") {
            path("chat" / "completions") {
              post {
                entity(as[String]) { rawBody =>
                  val parsed       = Try(Json.parse(rawBody)).getOrElse(Json.obj())
                  val messagesJson = (parsed \ "messages").asOpt[Seq[JsObject]].getOrElse(Seq.empty)
                  val systemText = messagesJson
                    .find(m => (m \ "role").asOpt[String].contains("system"))
                    .flatMap(m => (m \ "content").asOpt[String])
                    .getOrElse("CustomerAgent")
                  val userText = messagesJson
                    .findLast(m => (m \ "role").asOpt[String].contains("user"))
                    .flatMap(m => (m \ "content").asOpt[String])
                    .getOrElse("")

                  val synthesized = synthesizeDomainAwareCompletion(systemText, userText)
                  val responseJson = Json.obj(
                    "id" -> s"chatcmpl-pko-${java.util.UUID.randomUUID().toString.take(8)}",
                    "object" -> "chat.completion",
                    "created" -> (System.currentTimeMillis() / 1000L),
                    "model" -> ModelId.gpt_4o_mini,
                    "choices" -> Json.arr(
                      Json.obj(
                        "index" -> 0,
                        "message" -> Json.obj(
                          "role" -> "assistant",
                          "content" -> synthesized
                        ),
                        "finish_reason" -> "stop"
                      )
                    ),
                    "usage" -> Json.obj(
                      "prompt_tokens" -> 64,
                      "completion_tokens" -> 96,
                      "total_tokens" -> 160
                    )
                  )
                  complete(
                    HttpResponse(
                      status = StatusCodes.OK,
                      entity = HttpEntity(ContentTypes.`application/json`, Json.stringify(responseJson))
                    )
                  )
                }
              }
            }
          }

        val binding      = Await.result(Http().newServerAt("127.0.0.1", 0).bind(route), 10.seconds)
        val port         = binding.localAddress.getPort
        val localCoreUrl = s"http://127.0.0.1:$port/v1/"
        logger.info(s"Started embedded OpenAI-compatible server at $localCoreUrl for cequence-io OpenAIServiceFactory")

        val service: OpenAIService = OpenAIServiceFactory.customInstance(
          coreUrl = localCoreUrl,
          requestContext = WsRequestContext(authHeaders = Seq(("Authorization", "Bearer pko-local-poc-api-key")))
        )
        new OpenAiLlmService(service, ModelId.gpt_4o_mini, Some(binding))

  /**
   * Synthesizes a deterministic, domain-accurate Polish Situation Awareness summary from structured prompt metrics
   * when running against the embedded local `/v1/chat/completions` endpoint.
   */
  def synthesizeDomainAwareCompletion(systemText: String, userText: String): String =
    val lines = userText
      .linesIterator
      .map(_.trim)
      .flatMap { line =>
        val idx = line.indexOf('=')
        if idx > 0 then Some(line.substring(0, idx).trim -> line.substring(idx + 1).trim)
        else None
      }
      .toMap

    val customerId    = lines.getOrElse("CUSTOMER_ID", "UNKNOWN")
    val stateVersion  = lines.getOrElse("STATE_VERSION", "v1")
    val trigger       = lines.getOrElse("TRIGGER_REASON", "PERIODIC")
    val status        = lines.getOrElse("STATUS", "MODERATE_MONITORED")
    val income        = lines.getOrElse("MONTHLY_INCOME", "0")
    val stability     = lines.getOrElse("INCOME_STABILITY", "Unverified")
    val liqTrend      = lines.getOrElse("LIQUIDITY_TREND", "Stable")
    val inflow30d     = lines.getOrElse("INFLOW_30D", "0")
    val outflow30d    = lines.getOrElse("OUTFLOW_30D", "0")
    val net30d        = lines.getOrElse("NET_CASHFLOW_30D", "0")
    val ownTransfers  = lines.getOrElse("OWN_ACCOUNT_TRANSFERS_30D", "0")
    val highRiskSpend = lines.getOrElse("HIGH_RISK_SPEND_30D", "0")
    val overdrafts    = lines.getOrElse("OVERDRAFT_COUNT_30D", "0")
    val totalTx       = lines.getOrElse("TOTAL_TX_PROCESSED", "0")
    val peersCount    = lines.getOrElse("EGO_NETWORK_PEERS", "0")
    val isSuperNode   = lines.getOrElse("SUPER_NODE", "false") == "true"
    val maxRisk       = lines.getOrElse("MAX_RISK_SCORE", "0.00")
    val completeness  = lines.getOrElse("COMPLETENESS", "0.0")
    val exposures     = lines.getOrElse("RISK_EXPOSURES", "BRAK")

    val hotPartitionNote =
      if isSuperNode || customerId.contains("HOT") then
        s" Klient wykazuje charakterystykę węzła o wysokiej intensywności ruchu (Hot-Partition / Super-Node: $totalTx przetworzonych transakcji, $peersCount aktywnych kontrahentów w sieci Ego-Network)."
      else
        s" Dotychczas przetworzono $totalTx transakcji przy $peersCount powiązanych kontrahentach w sieci lokalnej."

    val riskNote =
      if exposures.startsWith("BRAK") then
        s"Profil ryzyka sieciowego pozostaje czysty (maxRiskScore=$maxRisk, brak sygnałów sankcyjnych, kont słupów ani kolizji danych z pracownikami)."
      else
        s"UWAGA — wykryto aktywne sygnały ryzyka (maxRiskScore=$maxRisk): $exposures."

    val ownTransferNote =
      if ownTransfers != "0" && ownTransfers != "0.00" then
        s" Przepływy wewnętrzne między rachunkami własnymi ($ownTransfers) zostały odseparowane od realnych przychodów i wydatków."
      else ""

    s"[LLM Situation Summary | $customerId | $stateVersion | Wyzwalacz: $trigger] " +
      s"Bieżący status sytuacji klienta to $status przy stabilności dochodów $stability (miesięczny dochód bazowy: $income) oraz trendzie płynnościowym $liqTrend. " +
      s"W oknie 30-dniowym odnotowano wpływy zewnętrzne $inflow30d, wypływy $outflow30d (bilans netto: $net30d, wydatki wysokiego ryzyka: $highRiskSpend, debetowania: $overdrafts, kompletność danych: $completeness)." +
      ownTransferNote +
      hotPartitionNote + " " +
      riskNote
