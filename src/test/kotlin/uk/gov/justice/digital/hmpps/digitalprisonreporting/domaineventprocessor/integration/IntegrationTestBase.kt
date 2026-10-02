package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import com.microsoft.applicationinsights.TelemetryClient
import jakarta.persistence.EntityManager
import org.awaitility.Awaitility.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.springframework.test.web.reactive.server.WebTestClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest
import tools.jackson.databind.json.JsonMapper
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestrictionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer.setLocalStackProperties
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.wiremock.HmppsAuthMockServer
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.InboundMessageListener
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LaoDataUpdateService
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LaoReconciliationService
import uk.gov.justice.digital.hmpps.digitalprisonreportinglib.integration.wiremock.ProbationIntegrationLaoMockServer
import uk.gov.justice.hmpps.sqs.HmppsQueueService
import uk.gov.justice.hmpps.sqs.HmppsSqsProperties
import uk.gov.justice.hmpps.sqs.MissingQueueException
import uk.gov.justice.hmpps.sqs.countAllMessagesOnQueue
import uk.gov.justice.hmpps.test.kotlin.auth.JwtAuthorisationHelper

@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(JwtAuthorisationHelper::class, TestFlywayConfig::class)
@ActiveProfiles("test")
@AutoConfigureWebTestClient
abstract class IntegrationTestBase {

  protected val inboundQueue by lazy { hmppsQueueService.findByQueueId("inboundqueue") ?: throw MissingQueueException("HmppsQueue inboundqueue not found") }
  private val inboundTopic by lazy { hmppsQueueService.findByTopicId("inboundtopic") ?: throw MissingQueueException("HmppsTopic inboundtopic not found") }

  protected val inboundSqsClient by lazy { inboundQueue.sqsClient }
  protected val inboundSqsDlqClient by lazy { inboundQueue.sqsDlqClient as SqsAsyncClient }
  protected val inboundSnsClient by lazy { inboundTopic.snsClient }

  protected val inboundQueueUrl by lazy { inboundQueue.queueUrl }
  protected val inboundDlqUrl by lazy { inboundQueue.dlqUrl as String }

  protected val inboundTopicArn by lazy { inboundTopic.arn }

  @Autowired
  protected lateinit var jsonMapper: JsonMapper

  @Autowired
  protected lateinit var jwtAuthHelper: JwtAuthorisationHelper

  @Autowired
  protected lateinit var hmppsQueueService: HmppsQueueService

  @MockitoSpyBean
  protected lateinit var laoExclusionRepository: LaoExclusionRepository

  @MockitoSpyBean
  protected lateinit var laoRestrictionRepository: LaoRestrictionRepository

  @MockitoSpyBean
  protected lateinit var laoReconciliationService: LaoReconciliationService

  @MockitoSpyBean
  protected lateinit var laoDataUpdateService: LaoDataUpdateService

  @Autowired
  protected lateinit var entityManager: EntityManager

  @Autowired
  protected lateinit var jdbcTemplate: JdbcTemplate

  @MockitoSpyBean
  protected lateinit var laoCrnRepository: LaoCrnRepository

  @MockitoSpyBean
  protected lateinit var hmppsSqsPropertiesSpy: HmppsSqsProperties

  @MockitoSpyBean
  protected lateinit var messageListener: InboundMessageListener

  @MockitoSpyBean
  protected lateinit var telemetryClient: TelemetryClient

  @Autowired
  lateinit var webTestClient: WebTestClient

  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction> = laoRestrictionRepository.findAll().filter { it.crn == crn }

  fun getLaoExclusionsForCrn(crn: String): List<LaoExclusion> = laoExclusionRepository.findAll().filter { it.crn == crn }

  protected fun jsonString(any: Any): String? = jsonMapper.writeValueAsString(any)

  internal fun setAuthorisation(
    username: String? = "AUTH_ADM",
    roles: List<String> = listOf(),
    scopes: List<String> = listOf("read"),
  ): (HttpHeaders) -> Unit = jwtAuthHelper.setAuthorisationHeader(username = username, scope = scopes, roles = roles)

  @BeforeEach
  fun setup() {
    inboundSqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundQueueUrl).build()).join()
    inboundSqsDlqClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundDlqUrl).build()).join()
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }
    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 0 }
    hmppsAuthMockServer.resetRequests()
    hmppsAuthMockServer.stubGrantToken()
  }

  companion object {
    private val localStackContainer = LocalStackContainer.instance

    @JvmField
    val hmppsAuthMockServer = HmppsAuthMockServer()

    @JvmStatic
    @DynamicPropertySource
    fun testcontainers(registry: DynamicPropertyRegistry) {
      localStackContainer?.also { setLocalStackProperties(it, registry) }
      pgContainer?.run {
        registry.add("spring.datasource.url", pgContainer::getJdbcUrl)
        registry.add("spring.datasource.username", pgContainer::getUsername)
        registry.add("spring.datasource.password", pgContainer::getPassword)
      }
    }

    val pgContainer = PostgresContainer.instance
    val probationIntegrationLaoMockServer = ProbationIntegrationLaoMockServer()

    @BeforeAll
    @JvmStatic
    fun setupClass() {
      probationIntegrationLaoMockServer.start()
      hmppsAuthMockServer.start()
    }

    @AfterAll
    @JvmStatic
    fun teardownClass() {
      probationIntegrationLaoMockServer.stop()
      hmppsAuthMockServer.stop()
    }
  }
}
