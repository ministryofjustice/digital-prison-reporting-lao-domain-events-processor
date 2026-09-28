package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class TransactionalRunner {
  @Transactional(rollbackFor = [Exception::class])
  fun <T> run(block: () -> T): T = block()
}