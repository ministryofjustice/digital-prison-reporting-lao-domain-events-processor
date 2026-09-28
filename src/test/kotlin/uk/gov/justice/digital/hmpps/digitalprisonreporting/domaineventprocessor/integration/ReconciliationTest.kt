package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.toLaoEntry
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

class ReconciliationTest : IntegrationTestBase() {

  companion object {
//    @JvmStatic
//    @DynamicPropertySource
//    fun registerProperties(registry: DynamicPropertyRegistry) {
//      registry.add("batch.enabled") { "true" }
//      registry.add("batch.type") { "reconciliation" }
//    }

    @BeforeAll
    fun setupWiremock() {
      probationIntegrationLaoMockServer.stubGetAllCases(payload = """
      {
        "content": [
          {
            "crn": "A111111",
            "username": "usera",
            "type": "exclusion",
            "exclusionMessage": "an exclusion message",
            "restrictionMessage": "a restriction message",
            "startDate": "2026-01-01T12:00:00.000Z",
            "endDate": "2026-01-01T13:00:00.000Z"
          }
        ],
        "page": {
          "size": 0,
          "number": 0,
          "totalElements": 0,
          "totalPages": 0
        }
      }
    """.trimIndent())
    }
  }

  @Test
  fun `should run the reconciliation`() {
    laoReconciliationService.reconcile()
    await().untilAsserted {

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