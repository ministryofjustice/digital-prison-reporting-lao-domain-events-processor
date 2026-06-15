package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class LaoDomainEventsProcessor

fun main(args: Array<String>) {
  runApplication<LaoDomainEventsProcessor>(*args)
}
