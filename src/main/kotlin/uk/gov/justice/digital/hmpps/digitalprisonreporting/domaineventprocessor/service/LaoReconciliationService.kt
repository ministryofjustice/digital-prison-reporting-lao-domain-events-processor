package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

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
  fun reconcile() {
    val allLaoData = laoDataProbationIntegrationClient.getAllLaoData()
    val crnDataMap = allLaoData.groupBy { it.crn }
    transactionalRunner.run {
      crnDataMap.forEach {
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
