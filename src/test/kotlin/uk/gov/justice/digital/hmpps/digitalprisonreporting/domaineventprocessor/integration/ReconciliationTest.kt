package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.toLaoEntry
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit

class ReconciliationTest : IntegrationTestBase() {

  @ParameterizedTest
  @CsvSource(
    "0,0,0",
    "1,0,0",
    "1,1,0",
    "1,1,1",
    "1,0,1",
    "0,1,1",
    "0,0,1",
  )
  fun `should run the reconciliation for a single page`(size: String, number: String, totalElements: String) {
    probationIntegrationLaoMockServer.stubGetAllCases(payload = """
      {
        "content": [
          {
            "crn": "A111111",
            "username": "usera",
            "type": "exclusion",
            "exclusionMessage": "an exclusion message",
            "restrictionMessage": "a restriction message",
            "startDate": "2026-01-01T12:00:00+01:00",
            "endDate": "2026-01-01T13:00:00+01:00"
          }
        ],
        "page": {
          "size": $size,
          "number": $number,
          "totalElements": $totalElements,
          "totalPages": 1
        }
      }
    """.trimIndent())
    laoReconciliationService.reconcile()
    await().timeout(Duration.of(5, ChronoUnit.SECONDS)).untilAsserted {

      val exclusions = getLaoExclusionsForCrn("A111111")
      assertThat(exclusions.size).isEqualTo(1)

      assertThat(exclusions.first().toLaoEntry()).satisfies(
        {
          assertThat(it.crn).isEqualTo("A111111")
          assertThat(it.userId).isEqualTo("usera")
          assertThat(it.reason).isEqualTo("an exclusion message")
          assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
          assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
        },
      )
      assertThat(getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    }
  }

  @ParameterizedTest
  @CsvSource(
    "0,0,0",
    "1,0,0",
    "1,1,0",
    "1,1,1",
    "1,0,1",
    "0,1,1",
    "0,0,1",
  )
  fun `should run the reconciliation for multiple pages`(size: String, number: String, totalElements: String) {
    probationIntegrationLaoMockServer.stubGetAllCases(
      page = 0,
      payload = """
      {
        "content": [
          {
            "crn": "A111111",
            "username": "usera",
            "type": "exclusion",
            "exclusionMessage": "an exclusion message",
            "restrictionMessage": "a restriction message",
            "startDate": "2026-01-01T12:00:00+01:00",
            "endDate": "2026-01-01T13:00:00+01:00"
          }
        ],
        "page": {
          "size": $size,
          "number": $number,
          "totalElements": $totalElements,
          "totalPages": 2
        }
      }
    """.trimIndent())

    probationIntegrationLaoMockServer.stubGetAllCases(
      page = 1,
      payload = """
      {
        "content": [
          {
            "crn": "A111111",
            "username": "userb",
            "type": "exclusion",
            "exclusionMessage": "an exclusion message",
            "restrictionMessage": "a restriction message",
            "startDate": "2026-01-01T12:00:00+01:00",
            "endDate": "2026-01-01T13:00:00+01:00"
          }
        ],
        "page": {
          "size": $size,
          "number": $number,
          "totalElements": $totalElements,
          "totalPages": 2
        }
      }
    """.trimIndent())
    laoReconciliationService.reconcile()
    await().timeout(Duration.of(5, ChronoUnit.SECONDS)).untilAsserted {

      val exclusions = getLaoExclusionsForCrn("A111111")
      assertThat(exclusions.size).isEqualTo(2)


      assertThat(exclusions).anySatisfy(
        {
          assertThat(it.crn).isEqualTo("A111111")
          assertThat(it.userId).isEqualTo("usera")
          assertThat(it.reason).isEqualTo("an exclusion message")
          assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
          assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
        },
      )
      assertThat(exclusions).anySatisfy(
        {
          assertThat(it.crn).isEqualTo("A111111")
          assertThat(it.userId).isEqualTo("userb")
          assertThat(it.reason).isEqualTo("an exclusion message")
          assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
          assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
        },
      )
      assertThat(getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    }
  }

  @Test
  fun `should run the reconciliation for multiple pages where the last is empty`() {
    probationIntegrationLaoMockServer.stubGetAllCases(
      page = 0,
      payload = """
      {
        "content": [
          {
            "crn": "A111111",
            "username": "usera",
            "type": "exclusion",
            "exclusionMessage": "an exclusion message",
            "restrictionMessage": "a restriction message",
            "startDate": "2026-01-01T12:00:00+01:00",
            "endDate": "2026-01-01T13:00:00+01:00"
          }
        ],
        "page": {
          "size": 1,
          "number": 1,
          "totalElements": 1,
          "totalPages": 2
        }
      }
    """.trimIndent())

    probationIntegrationLaoMockServer.stubGetAllCases(
      page = 1,
      payload = """
      {
        "content": [],
        "page": {
          "size": 1,
          "number": 1,
          "totalElements": 1,
          "totalPages": 2
        }
      }
    """.trimIndent())
    laoReconciliationService.reconcile()
    await().timeout(Duration.of(5, ChronoUnit.SECONDS)).untilAsserted {

      val exclusions = getLaoExclusionsForCrn("A111111")
      assertThat(exclusions.size).isEqualTo(1)

      assertThat(exclusions.first().toLaoEntry()).satisfies(
        {
          assertThat(it.crn).isEqualTo("A111111")
          assertThat(it.userId).isEqualTo("usera")
          assertThat(it.reason).isEqualTo("an exclusion message")
          assertThat(it.since).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 12, 0, 0), ZoneId.of("+01:00")))
          assertThat(it.until).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2026, 1, 1, 13, 0, 0), ZoneId.of("+01:00")))
        },
      )
      assertThat(getLaoRestrictionsForCrn("A111111").size).isEqualTo(0)
    }
  }
}