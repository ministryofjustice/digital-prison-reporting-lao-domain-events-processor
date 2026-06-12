package uk.gov.justice.digital.hmpps.digitalprisonreportinglib.integration.wiremock

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoApiDataEntry
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataResponse
import java.time.LocalDateTime

const val PROBATION_INTEGRATION_LAO_WIREMOCK_PORT = 8082

class ProbationIntegrationLaoMockServer : MockServer(PROBATION_INTEGRATION_LAO_WIREMOCK_PORT) {
  fun stubGetLaoDataForCrn(
    payload: String,
    crn: String = "A111111",
  ) {
    stubFor(
      get("$urlPrefix/case/$crn/access")
        .willReturn(
          aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(payload).withStatus(200),
        ),
    )
  }

  fun stub500ResponseInitial(url: String) {
    stubFor(
      get("$urlPrefix/$url")
        .inScenario("retry")
        .whenScenarioStateIs(STARTED)
        .willReturn(aResponse().withStatus(500))
        .willSetStateTo("second"),
    )
  }

  fun stub500ResponseSecondRequest(url: String) {
    stubFor(
      get("$urlPrefix/$url")
        .inScenario("retry")
        .whenScenarioStateIs("second")
        .willReturn(aResponse().withStatus(500))
        .willSetStateTo("success"),
    )
  }

  fun stubSuccessInThirdRequest(url: String) {
    stubFor(
      get("$urlPrefix/$url")
        .inScenario("retry")
        .whenScenarioStateIs("success")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(createPayload()),
        ),
    )
  }

  private fun createPayload(): String = """
      {
        "excludedFrom": [
          {
            "username": "A111111",
            "since": "2026-06-10T12:00:00",
            "until": "2026-06-10T13:00:00"
          }
        ],
        "restrictedTo": [
          {
            "username": "A111222",
            "since": "2026-06-10T12:00:00",
            "until": "2026-06-10T13:00:00"
          }
        ],
        "exclusionMessage": "Excluded!",
        "restrictionMessage": "Restricted"
      }
    """.trimIndent()
}
