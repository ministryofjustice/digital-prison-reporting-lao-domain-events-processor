package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.toLaoEntry
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.toExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.toRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LAOEvent
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service.LaoDataType
import uk.gov.justice.hmpps.sqs.countAllMessagesOnQueue
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

class QueueTest : IntegrationTestBase() {
  @Test
  fun `new restriction is added`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:00:00.000000000+01:00"
          }
        ],
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:00:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }

    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(1)
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(1)

    val restriction = restrictions.first().toLaoEntry()
    assertThat(restriction).isEqualTo(
      LaoEntry(
        "A111111",
        "usera",
        "Restricted",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ),
    )
  }

  @Test
  fun `new exclusion is added`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:00:00.000000000+01:00"
          },
          {
            "username": "userb",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Exclusion)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }

//    await().atLeast(Duration.ofMillis(1000)).await()
//      .untilAsserted {
    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(2)

    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })

    assertThat(exclusions[1].toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("userb")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 30, 0), ZoneId.of("+01:00")))
    })
//    }
  }

  @Test
  fun `exclusion is removed`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Exclusion)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }
//    await().atMost(2, TimeUnit.SECONDS).until { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() == 0 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
  }

  @Test
  fun `restriction is removed`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    laoRestrictionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Restricted",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toRestriction(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:00:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(1)
  }

  @Test
  fun `restriction is updated`() = runTest {
    laoRestrictionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Restricted",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toRestriction(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }

    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(1)
    assertThat(restrictions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 30, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `exclusion is updated`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(1)
    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 30, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `two exclusion additions should throw an error`() = runTest {
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          },
          {
            "username": "userb",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
  }

  @Test
  fun `two restriction additions should throw an error`() = runTest {
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          },
          {
            "username": "userb",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
  }

  @Test
  fun `a restriction and exclusion addition should throw an error`() = runTest {
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:00:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
  }

  @Test
  fun `two restriction updates should throw an error`() = runTest {
    laoRestrictionRepository.saveAllAndFlush(
      listOf(
        LaoEntry(
          "A111111",
          "usera",
          "Restricted",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toRestriction(),
        LaoEntry(
          "A111111",
          "userb",
          "Restricted",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toRestriction(),
      ),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          },
          {
            "username": "userb",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(2)
    assertThat(restrictions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    assertThat(restrictions[1].toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("userb")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `two exclusion updates should throw an error`() = runTest {
    laoExclusionRepository.saveAllAndFlush(
      listOf(
        LaoEntry(
          "A111111",
          "usera",
          "Excluded!",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toExclusion(),
        LaoEntry(
          "A111111",
          "userb",
          "Excluded!",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toExclusion(),
      ),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          },
          {
            "username": "userb",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 1 }
    await().untilCallTo { inboundSqsClient.countAllMessagesOnQueue(inboundQueueUrl).get() } matches { it == 0 }
    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(2)
    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    assertThat(exclusions[1].toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("userb")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `a restriction and exclusion update should throw an error`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    laoRestrictionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Restricted",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toRestriction(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "restrictedTo": [
          {
            "username": "usera",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "excludedFrom": [
          {
            "username": "usera",
            "since": "2026-01-01T12:30:00.000000000+01:00",
            "until": "2026-01-01T13:30:00.000000000+01:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(1)
    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(1)
    assertThat(restrictions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `two restriction deletions should throw an error`() = runTest {
    laoRestrictionRepository.saveAllAndFlush(
      listOf(
        LaoEntry(
          "A111111",
          "usera",
          "Restricted",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toRestriction(),
        LaoEntry(
          "A111111",
          "userb",
          "Restricted",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toRestriction(),
      ),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoExclusionRepository.getLaoExclusionsForCrn("A111111").size).isEqualTo(0)
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(2)
    assertThat(restrictions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    assertThat(restrictions[1].toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("userb")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `two exclusion deletions should throw an error`() = runTest {
    laoExclusionRepository.saveAllAndFlush(
      listOf(
        LaoEntry(
          "A111111",
          "usera",
          "Excluded!",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toExclusion(),
        LaoEntry(
          "A111111",
          "userb",
          "Excluded!",
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
          ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
        ).toExclusion(),
      ),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    assertThat(laoRestrictionRepository.getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(2)
    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    assertThat(exclusions[1].toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("userb")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  @Test
  fun `a restriction and exclusion deletion should throw an error`() = runTest {
    laoExclusionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Excluded!",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toExclusion(),
    )
    laoRestrictionRepository.saveAndFlush(
      LaoEntry(
        "A111111",
        "usera",
        "Restricted",
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")),
        ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")),
      ).toRestriction(),
    )
    probationIntegrationLaoMockServer.stubGetLaoDataForCrn(
      """
      {
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
      """.trimIndent(),
    )
    publishLaoEvent(LaoDataType.Restriction)

    await().untilCallTo { inboundSqsDlqClient.countAllMessagesOnQueue(inboundDlqUrl).get() } matches { it == 1 }

    val exclusions = laoExclusionRepository.getLaoExclusionsForCrn("A111111")
    assertThat(exclusions.size).isEqualTo(1)
    assertThat(exclusions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Excluded!")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
    val restrictions = laoRestrictionRepository.getLaoRestrictionsForCrn("A111111")
    assertThat(restrictions.size).isEqualTo(1)
    assertThat(restrictions.first().toLaoEntry()).satisfies({
      assertThat(it.crn).isEqualTo("A111111")
      assertThat(it.userId).isEqualTo("usera")
      assertThat(it.reason).isEqualTo("Restricted")
      assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 30, 0), ZoneId.of("+01:00")))
      assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
    })
  }

  private fun publishLaoEvent(type: LaoDataType) {
    val event = LAOEvent(
      "probation-case.${type.name.lowercase()}.updated",
      1,
      "",
      LocalDateTime.now(),
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
