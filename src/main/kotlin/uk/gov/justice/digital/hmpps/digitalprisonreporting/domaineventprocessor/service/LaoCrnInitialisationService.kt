package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrn
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository

@Service
class LaoCrnInitialisationService(
  private val laoCrnRepository: LaoCrnRepository,
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  fun insertCrnIfNeeded(crn: String) {
    val crns = laoCrnRepository.findByCrn(crn)
    log.info(
      "CRN {} returned {} LaoCrn records",
      crn,
      crns.size,
    )
    when (crns.size) {
      0 -> {
        laoCrnRepository.save(LaoCrn(crn = crn, version = 0, laoRestrictions = mutableSetOf(), laoExclusions = mutableSetOf()))
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
