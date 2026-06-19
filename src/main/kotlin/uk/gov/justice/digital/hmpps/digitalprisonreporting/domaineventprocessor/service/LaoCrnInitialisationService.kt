package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrn
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository

@Service
class LaoCrnInitialisationService(
  private val laoCrnRepository: LaoCrnRepository,
) {
  fun insertCrnIfNeeded(crn: String) {
    val crns = laoCrnRepository.findByCrn(crn)
    when (crns.size) {
      0 -> {
        laoCrnRepository.save(LaoCrn(null, crn, 0, mutableSetOf(), mutableSetOf()))
        deleteExtraCrns(laoCrnRepository.findByCrn(crn))
      }
      1 -> return
      else -> deleteExtraCrns(crns)
    }
  }

  private fun deleteExtraCrns(crns: Collection<LaoCrn>) {
    laoCrnRepository.deleteAll(crns.sortedBy { it.lastUpdated }.drop(1))
  }
}