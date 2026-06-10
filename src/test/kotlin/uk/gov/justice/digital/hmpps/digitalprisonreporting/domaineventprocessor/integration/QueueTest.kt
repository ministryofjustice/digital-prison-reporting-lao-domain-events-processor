package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LAOEvent

import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import uk.gov.justice.hmpps.sqs.SnsMessage
import uk.gov.justice.hmpps.sqs.countMessagesOnQueue

class QueueTest: IntegrationTestBase() {
  @Test
  fun `event is published to outbound topic`() = runTest {
    String::class.java
    val event = LAOEvent(
      "probation-case.restriction.updated",
      1,
      "",
      Date(),
      LAOEvent.PersonReference(
        listOf(
          LAOEvent.PersonReference.Identifier("CRN", "A111111")
        )
      )
    )
    inboundSnsClient.publish(
      PublishRequest.builder().topicArn(inboundTopicArn).message(jsonString(event)).messageAttributes(
        mapOf("eventType" to MessageAttributeValue.builder().dataType("String").stringValue(event.type).build()),
      ).build(),
    )

  }
}