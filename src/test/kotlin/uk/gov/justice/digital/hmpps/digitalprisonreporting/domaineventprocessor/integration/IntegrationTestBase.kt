package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import com.microsoft.applicationinsights.TelemetryClient
import io.awspring.cloud.sqs.listener.MessageListener
import jakarta.persistence.EntityManager
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.awaitility.Awaitility.await
import org.awaitility.kotlin.has
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.springframework.test.web.reactive.server.WebTestClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest
import tools.jackson.databind.json.JsonMapper
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrn
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestrictionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.mocks.OAuthExtension
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer.setLocalStackProperties
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.wiremock.HmppsAuthApiExtension.Companion.hmppsAuth
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.InboundMessageListener
import uk.gov.justice.digital.hmpps.digitalprisonreportinglib.integration.wiremock.ProbationIntegrationLaoMockServer
import uk.gov.justice.hmpps.sqs.HmppsQueueService
import uk.gov.justice.hmpps.sqs.HmppsSqsProperties
import uk.gov.justice.hmpps.sqs.MessageAttribute
import uk.gov.justice.hmpps.sqs.MessageAttributes
import uk.gov.justice.hmpps.sqs.MissingQueueException
import uk.gov.justice.hmpps.sqs.MissingTopicException
import uk.gov.justice.hmpps.sqs.countAllMessagesOnQueue
import uk.gov.justice.hmpps.test.kotlin.auth.JwtAuthorisationHelper

@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(JwtAuthorisationHelper::class, TestFlywayConfig::class)
@ExtendWith(OAuthExtension::class)
@ActiveProfiles("test")
@AutoConfigureWebTestClient
abstract class IntegrationTestBase {

  fun HmppsSqsProperties.inboundQueueConfig() = queues["inboundqueue"] ?: throw MissingQueueException("inboundqueue has not been loaded from configuration properties")

  fun HmppsSqsProperties.inboundTopicConfig() = topics["inboundtopic"] ?: throw MissingTopicException("inboundtopic has not been loaded from configuration properties")

  protected val inboundQueue by lazy { hmppsQueueService.findByQueueId("inboundqueue") ?: throw MissingQueueException("HmppsQueue inboundqueue not found") }
  protected val inboundQueueDlq by lazy { hmppsQueueService.findByQueueName("inbound-dlq") ?: throw MissingQueueException("InboundDlq does not exist") }
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

  @Autowired
  protected lateinit var laoExclusionRepository: LaoExclusionRepository

  @Autowired
  protected lateinit var laoRestrictionRepository: LaoRestrictionRepository

  @Autowired
  protected lateinit var entityManager: EntityManager

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

  internal fun HttpHeaders.authToken(roles: List<String> = listOf("ROLE_QUEUE_ADMIN")) {
    this.setBearerAuth(
      jwtAuthHelper.createJwtAccessToken(
        username = "SOME_USER",
        roles = roles,
        clientId = "some-client",
      ),
    )
  }

  protected fun jsonString(any: Any): String? = jsonMapper.writeValueAsString(any)

  internal fun setAuthorisation(
    username: String? = "AUTH_ADM",
    roles: List<String> = listOf(),
    scopes: List<String> = listOf("read"),
  ): (HttpHeaders) -> Unit = jwtAuthHelper.setAuthorisationHeader(username = username, scope = scopes, roles = roles)

  @BeforeEach
  fun setup()  {
    laoCrnRepository.deleteAll()
    laoRestrictionRepository.deleteAll()
    laoExclusionRepository.deleteAll()
    probationIntegrationLaoMockServer.resetAll()
    inboundSqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundQueueUrl).build()).join()
    inboundSqsDlqClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundDlqUrl).build()).join()
    await().untilCallTo { laoExclusionRepository.count() } matches { it == 0L }
    await().untilCallTo { laoRestrictionRepository.count() } matches { it == 0L }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }
    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 0 }
  }

  companion object {
    private val localStackContainer = LocalStackContainer.instance

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
    }

    @AfterAll
    @JvmStatic
    fun teardownClass() {
      probationIntegrationLaoMockServer.stop()
    }
  }
}
