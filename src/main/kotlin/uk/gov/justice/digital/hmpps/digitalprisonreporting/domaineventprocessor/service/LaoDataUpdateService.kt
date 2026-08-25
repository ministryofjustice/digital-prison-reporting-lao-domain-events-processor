package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestrictionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.SerializableIsolationViolationException
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.toRetryableExceptionIfRequired
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient
import java.time.LocalDateTime

@Service
@Transactional
class LaoDataUpdateService(
  private val laoCrnRepository: LaoCrnRepository,
  private val laoExclusionRepository: LaoExclusionRepository,
  private val laoRestrictionRepository: LaoRestrictionRepository,
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
) {

  // This is just a Redshift locking issue, it doesn't like when multiple deletes happen at the same time, and the entire process
  // is idempotent and transaction wrapped, so there's no issue with retrying a large number of times. Rather retry and get it
  // eventually than have messages on the DLQ for this issue.
  @Retryable(
    retryFor = [SerializableIsolationViolationException::class],
    maxAttempts = 20,
    backoff = Backoff(delay = 1000),
  )
  fun process(crn: String) {
    val liveLaoData = laoDataProbationIntegrationClient.getLaoData(crn)
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoExclusion(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until, "$crn:${it.username}") }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoRestriction(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until, "$crn:${it.username}") }

    val laoCrn = laoCrnRepository.findByCrn(crn).single()
    try {
      laoExclusionRepository.deleteByCrn(crn)
      laoRestrictionRepository.deleteByCrn(crn)
      laoExclusionRepository.saveAll(liveLaoDataTransformedExclusions)
      laoRestrictionRepository.saveAll(liveLaoDataTransformedRestrictions)
    } catch (e: Exception) {
      throw e.toRetryableExceptionIfRequired() ?: e
    }

    laoCrn.lastUpdated = LocalDateTime.now()
    laoCrnRepository.save(laoCrn)
  }
}
