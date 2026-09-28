package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import kotlinx.coroutines.runBlocking
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient

//@ConditionalOnProperty(name = ["batch.enabled"], havingValue = "true")
//@ConditionalOnProperty(name = ["batch.type"], havingValue = "reconciliation")
@Service
class LaoReconciliationService(
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val laoCrnInitialisationService: LaoCrnInitialisationService,
  private val laoDataUpdateService: LaoDataUpdateService,
  private val transactionalRunner: TransactionalRunner,
) {
  @EventListener
  fun onApplicationEvent(event: ContextRefreshedEvent) = runBlocking {
    reconcile()
  }.also { event.closeApplication() }

  fun reconcile() {
    val allLaoData = laoDataProbationIntegrationClient.getAllLaoData()
    val crnDataMap = allLaoData.groupBy { it.crn }
    transactionalRunner.run {
      crnDataMap.forEach {
        laoCrnInitialisationService.insertCrnIfNeeded(it.key)
        val exclusions = it.value
          .filter { entry -> entry.type.lowercase() == "exclusion" }
          .map { entry -> LaoExclusion(it.key, entry.username, entry.exclusionMessage, entry.since, entry.until, "${it.key}:${entry.username}") }
        val restrictions = it.value
          .filter { entry -> entry.type.lowercase() == "restriction" }
          .map { entry -> LaoRestriction(it.key, entry.username, entry.exclusionMessage, entry.since, entry.until, "${it.key}:${entry.username}") }
        laoDataUpdateService.saveLaoDataForCrn(it.key, exclusions, restrictions)
      }
    }
  }

  private fun ContextRefreshedEvent.closeApplication() = (this.applicationContext as ConfigurableApplicationContext).close()
}