package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration

import io.netty.channel.ConnectTimeoutException
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.handler.timeout.TimeoutException
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.io.IOException
import java.time.Duration
import java.util.Date

class LaoDataProbationIntegrationClient(
  private val laoDataProbationIntegrationClient: WebClient,
) {
  fun getLaoData(crn: String): LaoDataResponse = laoDataProbationIntegrationClient.get()
    .uri("/case/$crn/access")
    .header("Content-Type", "application/json")
    .retrieve()
    .bodyToMono<LaoDataResponse>(LaoDataResponse::class.java)
    .retryWhen(retryWithExponentialBackOffAndJitter)
    .block()!!

  private val retryWithExponentialBackOffAndJitter = Retry
    .backoff(3, Duration.ofMillis(500))
    .maxBackoff(Duration.ofSeconds(5))
    .jitter(0.5)
    .filter { throwable ->
      when (throwable) {
        is WebClientResponseException ->
          throwable.statusCode.is5xxServerError

        is WebClientRequestException,
        is ConnectTimeoutException,
        is ReadTimeoutException,
        is TimeoutException,
        is IOException,
        -> true

        else -> false
      }
    }
}

data class LaoApiDataEntry(
  val username: String,
  val since: Date,
  val until: Date?,
)

data class LaoDataResponse(
  val excludedFrom: List<LaoApiDataEntry>,
  val restrictedTo: List<LaoApiDataEntry>,
  val exclusionMessage: String,
  val restrictionMessage: String,
)
