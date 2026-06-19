package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

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
  fun process(crn: String) {
    val liveLaoData = laoDataProbationIntegrationClient.getLaoData(crn)
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoExclusion(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until, "$crn:${it.username}") }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoRestriction(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until, "$crn:${it.username}") }

    val laoCrn = laoCrnRepository.findByCrn(crn).single()
    laoCrn.addExclusions(liveLaoDataTransformedExclusions)
    laoCrn.addRestrictions(liveLaoDataTransformedRestrictions)

    laoCrnRepository.save(laoCrn)
  }
}