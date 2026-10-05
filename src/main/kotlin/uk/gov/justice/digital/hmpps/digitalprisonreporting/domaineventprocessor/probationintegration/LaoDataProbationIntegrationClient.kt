package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration

import io.netty.channel.ConnectTimeoutException
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.handler.timeout.TimeoutException
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.io.IOException
import java.time.Duration
import java.time.ZonedDateTime
import kotlin.jvm.java

class LaoDataProbationIntegrationClient(
  private val laoDataProbationIntegrationClient: WebClient,
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  fun getLaoData(crn: String): LaoDataResponse = laoDataProbationIntegrationClient.get()
    .uri("/case/$crn/access")
    .header("Content-Type", "application/json")
    .retrieve()
    .bodyToMono(LaoDataResponse::class.java)
    .retryWhen(retryWithExponentialBackOffAndJitter)
    .block()!!

  data class LaoApiDataEntry(
    val username: String,
    val since: ZonedDateTime,
    val until: ZonedDateTime?,
  )

  data class LaoDataResponse(
    val excludedFrom: List<LaoApiDataEntry> = emptyList(),
    val restrictedTo: List<LaoApiDataEntry> = emptyList(),
    val exclusionMessage: String?,
    val restrictionMessage: String?,
  )

  fun getAllLaoData(): List<AllCasesContentEntry> {
    val firstRequest = getAllCasesPage(0).block()!!
    log.info("Processing ${firstRequest.page.totalPages} pages with ${firstRequest.page.totalElements}")
    val cases = firstRequest.content
    if (firstRequest.page.totalPages <= 1) {
      return cases
    }
    val pages = Flux.fromIterable((1..<firstRequest.page.totalPages))
      .buffer(10)
      .concatMap { batch ->
        Flux.fromIterable(batch)
          .flatMapSequential { getAllCasesPage(it) }
          .collectList()
      }
      .flatMapIterable { it }
      .collectList()
      .block()!!

    pages.add(firstRequest)
    return pages.flatMap({ it.content })
  }

  private fun getAllCasesPage(page: Int): Mono<AllCasesResponse> = laoDataProbationIntegrationClient.get()
    .uri("/all-cases?size=1000&page=$page")
    .header("Content-Type", "application/json")
    .retrieve()
    .bodyToMono(AllCasesResponse::class.java)
    .retryWhen(retryWithExponentialBackOffAndJitter)

  data class AllCasesResponse(
    val content: List<AllCasesContentEntry>,
    val page: Page,
  )

  data class Page(
    val size: Int,
    val number: Int,
    val totalElements: Int,
    val totalPages: Int,
  )

  data class AllCasesContentEntry(
    val crn: String,
    val username: String,
    val type: String,
    val exclusionMessage: String?,
    val restrictionMessage: String?,
    val startDate: ZonedDateTime,
    val endDate: ZonedDateTime?,
  )

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
