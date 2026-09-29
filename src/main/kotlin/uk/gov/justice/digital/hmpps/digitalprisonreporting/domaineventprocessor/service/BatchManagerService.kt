package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@ConditionalOnProperty(name = ["batch.enabled"], havingValue = "true")
@ConditionalOnProperty(name = ["batch.type"])
@Service
class BatchManagerService(
  private val laoReconciliationService: LaoReconciliationService,
  @Value($$"${batch.type}") private val batchType: String,
) {
  @EventListener
  fun onApplicationEvent(event: ContextRefreshedEvent) = runBlocking {
    if (batchType == "reconciliation") laoReconciliationService.reconcile()
  }.also { event.closeApplication() }

  private fun ContextRefreshedEvent.closeApplication() = (this.applicationContext as ConfigurableApplicationContext).close()
}