package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@ConditionalOnProperty(name = ["batch.enabled"], havingValue = "true")
@Service
class BatchManagerService(
  private val laoReconciliationService: LaoReconciliationService,
  @Value($$"${batch.type}") private val batchType: String,
) {
  @EventListener
  fun onApplicationEvent(event: ContextRefreshedEvent) = try {
    runBatchJob(batchType)
  } finally {
    event.closeApplication()
  }

  fun runBatchJob(batchType: String) {
    if (batchType == "reconciliation") laoReconciliationService.reconcile()
  }

  private fun ContextRefreshedEvent.closeApplication() = (this.applicationContext as ConfigurableApplicationContext).close()
}
