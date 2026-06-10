package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import com.microsoft.applicationinsights.TelemetryClient
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
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
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.mocks.OAuthExtension
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.testcontainers.LocalStackContainer.setLocalStackProperties
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.wiremock.HmppsAuthApiExtension.Companion.hmppsAuth
import uk.gov.justice.hmpps.sqs.HmppsQueueService
import uk.gov.justice.hmpps.sqs.HmppsSqsProperties
import uk.gov.justice.hmpps.sqs.MessageAttribute
import uk.gov.justice.hmpps.sqs.MessageAttributes
import uk.gov.justice.hmpps.sqs.MissingQueueException
import uk.gov.justice.hmpps.sqs.MissingTopicException
import uk.gov.justice.hmpps.test.kotlin.auth.JwtAuthorisationHelper

@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(JwtAuthorisationHelper::class)
@ExtendWith(OAuthExtension::class)
@ActiveProfiles("test")
@AutoConfigureWebTestClient
abstract class IntegrationTestBase {

  @BeforeEach
  fun `clear queues`() {
    inboundSqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundQueueUrl).build()).get()
    inboundSqsDlqClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundDlqUrl).build()).get()
    inboundSqsOnlyClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(inboundSqsOnlyQueueUrl).build()).get()
  }

  fun HmppsSqsProperties.inboundQueueConfig() = queues["inboundqueue"] ?: throw MissingQueueException("inboundqueue has not been loaded from configuration properties")

  fun HmppsSqsProperties.inboundTopicConfig() = topics["inboundtopic"] ?: throw MissingTopicException("inboundtopic has not been loaded from configuration properties")

  protected val inboundQueue by lazy { hmppsQueueService.findByQueueId("inboundqueue") ?: throw MissingQueueException("HmppsQueue inboundqueue not found") }
  private val inboundTopic by lazy { hmppsQueueService.findByTopicId("inboundtopic") ?: throw MissingQueueException("HmppsTopic inboundtopic not found") }
  private val inboundSqsOnlyQueue by lazy { hmppsQueueService.findByQueueId("inboundsqsonlyqueue") ?: throw MissingQueueException("HmppsQueue inboundsqsonlyqueue not found") }

  protected val inboundSqsClient by lazy { inboundQueue.sqsClient }
  protected val inboundSqsDlqClient by lazy { inboundQueue.sqsDlqClient as SqsAsyncClient }
  protected val inboundSnsClient by lazy { inboundTopic.snsClient }
  protected val inboundSqsOnlyClient by lazy { inboundSqsOnlyQueue.sqsClient }

  protected val inboundQueueUrl by lazy { inboundQueue.queueUrl }
  protected val inboundDlqUrl by lazy { inboundQueue.dlqUrl as String }
  protected val inboundSqsOnlyQueueUrl by lazy { inboundSqsOnlyQueue.queueUrl }

  protected val inboundTopicArn by lazy { inboundTopic.arn }

  @Autowired
  protected lateinit var jsonMapper: JsonMapper

  @Autowired
  protected lateinit var jwtAuthHelper: JwtAuthorisationHelper

  @Autowired
  protected lateinit var hmppsQueueService: HmppsQueueService

  @MockitoSpyBean
  protected lateinit var hmppsSqsPropertiesSpy: HmppsSqsProperties

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

  protected fun jsonString(any: Any) = jsonMapper.writeValueAsString(any)

  protected fun messageAttributesWithEventType(eventType: String): MessageAttributes = MessageAttributes().apply {
    put("eventType", MessageAttribute("String", eventType))
  }

  internal fun setAuthorisation(
    username: String? = "AUTH_ADM",
    roles: List<String> = listOf(),
    scopes: List<String> = listOf("read"),
  ): (HttpHeaders) -> Unit = jwtAuthHelper.setAuthorisationHeader(username = username, scope = scopes, roles = roles)

  protected fun stubPingWithResponse(status: Int) {
    hmppsAuth.stubHealthPing(status)
  }

  companion object {
    private val localStackContainer = LocalStackContainer.instance

    @JvmStatic
    @DynamicPropertySource
    fun testcontainers(registry: DynamicPropertyRegistry) {
      localStackContainer?.also { setLocalStackProperties(it, registry) }
    }
  }
}
