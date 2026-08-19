package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.orm.jpa.JpaSystemException
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient

@Service
@Transactional
class LaoDataUpdateService(
  private val laoCrnRepository: LaoCrnRepository,
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  @Retryable(
    retryFor = [JpaSystemException::class],
    maxAttempts = 3,
    backoff = Backoff(delay = 1000),
  )
  fun process(crn: String) {
    val t0 = System.currentTimeMillis()
    val liveLaoData = laoDataProbationIntegrationClient.getLaoData(crn)
    log.info("getLaoData took {}ms", System.currentTimeMillis() - t0)
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoExclusion(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until, "$crn:${it.username}") }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoRestriction(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until, "$crn:${it.username}") }

    log.info(
      "Updating CRN {} with {} exclusions and {} restrictions",
      crn,
      liveLaoDataTransformedExclusions.size,
      liveLaoDataTransformedRestrictions.size,
    )
    val t1 = System.currentTimeMillis()
    val laoCrn = laoCrnRepository.findByCrn(crn).single()
    log.info("findByCrn took {}ms", System.currentTimeMillis() - t1)
    laoCrn.addExclusions(liveLaoDataTransformedExclusions)
    laoCrn.addRestrictions(liveLaoDataTransformedRestrictions)

    val t2 = System.currentTimeMillis()
    laoCrnRepository.save(laoCrn)
    log.info("save took {}ms", System.currentTimeMillis() - t2)
  }
}
