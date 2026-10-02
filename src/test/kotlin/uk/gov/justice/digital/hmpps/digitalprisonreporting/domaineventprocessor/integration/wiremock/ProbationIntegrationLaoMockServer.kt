package uk.gov.justice.digital.hmpps.digitalprisonreportinglib.integration.wiremock

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED

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

  fun stubGetAllCases(
    page: Int = 0,
    payload: String,
  ) {
    stubFor(
      get("$urlPrefix/all-cases?size=1000&page=$page")
        .willReturn(
          aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(payload).withStatus(200),
        ),
    )
  }
}
