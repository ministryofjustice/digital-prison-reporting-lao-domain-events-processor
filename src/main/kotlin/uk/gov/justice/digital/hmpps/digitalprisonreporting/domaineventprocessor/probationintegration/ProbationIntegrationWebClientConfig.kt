package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.oauth2.client.AuthorizedClientServiceOAuth2AuthorizedClientManager
import org.springframework.security.oauth2.client.InMemoryOAuth2AuthorizedClientService
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProviderBuilder
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository
import org.springframework.web.reactive.function.client.WebClient
import uk.gov.justice.hmpps.kotlin.auth.authorisedWebClient
import java.time.Duration

@Configuration
class ProbationIntegrationWebClientConfig(
  @Value("\${laodata.host}")
  private val laoDataProbationIntegrationHost: String,
  @Value("\${api.timeout:20s}")
  private val healthTimeout: Duration,
) {

  @Bean
  fun authorizedClientManager(
    clientRegistration: ClientRegistrationRepository,
  ): OAuth2AuthorizedClientManager {
    val service = InMemoryOAuth2AuthorizedClientService(clientRegistration)
    val authorizedClientManager = AuthorizedClientServiceOAuth2AuthorizedClientManager(clientRegistration, service)

    val authorizedClientProvider = OAuth2AuthorizedClientProviderBuilder
      .builder()
      .clientCredentials()
      .build()
    authorizedClientManager.setAuthorizedClientProvider(authorizedClientProvider)
    return authorizedClientManager
  }

  @Bean
  fun laoDataProbationIntegrationWebClient(
    authorizedClientManager: OAuth2AuthorizedClientManager,
    webclientBuilder: WebClient.Builder,
  ): WebClient = webclientBuilder
    .authorisedWebClient(
      authorizedClientManager,
      registrationId = "LAO_DATA",
      url = laoDataProbationIntegrationHost,
      healthTimeout,
    )

  @Bean
  fun laoDataClient(
    laoDataProbationIntegrationWebClient: WebClient,
  ): LaoDataProbationIntegrationClient = LaoDataProbationIntegrationClient(
    laoDataProbationIntegrationWebClient,
  )
}
