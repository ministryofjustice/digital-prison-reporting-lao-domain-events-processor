package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient

@Service
class LaoReconciliationService(
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val laoCrnInitialisationService: LaoCrnInitialisationService,
  private val laoDataUpdateService: LaoDataUpdateService,
  private val transactionalRunner: TransactionalRunner,
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  fun reconcile() {
    val allLaoData = laoDataProbationIntegrationClient.getAllLaoData()
    val crnDataMap = allLaoData.groupBy { it.crn }
    log.info("Processing ${crnDataMap.keys.size} CRNs")
    transactionalRunner.run {
      for ((idx, it) in crnDataMap.entries.withIndex()) {
        if (idx % 10 == 0) {
          log.info("Processed index $idx")
        }
        laoCrnInitialisationService.insertCrnIfNeeded(it.key)
        val exclusions = it.value
          .filter { entry -> entry.type.lowercase() == "exclusion" }
          .map { entry -> LaoExclusion(it.key, entry.username, entry.exclusionMessage, entry.startDate, entry.endDate, "${it.key}:${entry.username}") }
        val restrictions = it.value
          .filter { entry -> entry.type.lowercase() == "restriction" }
          .map { entry -> LaoRestriction(it.key, entry.username, entry.restrictionMessage, entry.startDate, entry.endDate, "${it.key}:${entry.username}") }
        laoDataUpdateService.saveLaoDataForCrn(it.key, exclusions, restrictions)
      }
    }
  }
}
