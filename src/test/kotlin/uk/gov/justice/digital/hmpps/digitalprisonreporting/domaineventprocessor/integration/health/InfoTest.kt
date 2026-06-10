package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.health

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.info.BuildProperties
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.IntegrationTestBase

class InfoTest(
  @Autowired private val buildProperties: BuildProperties,
) : IntegrationTestBase() {
  @Test
  fun `Info page is accessible`() {
    webTestClient.get()
      .uri("/info")
      .exchange()
      .expectStatus()
      .isOk
      .expectBody()
      .jsonPath("build.name").isEqualTo("dpr-lao-domain-event-processor")
  }

  @Test
  fun `Info page reports version`() {
    webTestClient.get().uri("/info")
      .exchange()
      .expectStatus().isOk
      .expectBody().jsonPath("build.version").isEqualTo(buildProperties.version)
  }
}
