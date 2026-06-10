package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LAOEvent
import java.util.Date

class QueueTest : IntegrationTestBase() {
  @Test
  fun `event is published to outbound topic`() = runTest {
    val event = LAOEvent(
      "probation-case.restriction.updated",
      1,
      "",
      Date(),
      LAOEvent.PersonReference(
        listOf(
          LAOEvent.PersonReference.Identifier("CRN", "A111111"),
        ),
      ),
    )
    inboundSnsClient.publish(
      PublishRequest.builder().topicArn(inboundTopicArn).message(jsonString(event)).messageAttributes(
        mapOf("eventType" to MessageAttributeValue.builder().dataType("String").stringValue(event.eventType).build()),
      ).build(),
    )
  }
}
