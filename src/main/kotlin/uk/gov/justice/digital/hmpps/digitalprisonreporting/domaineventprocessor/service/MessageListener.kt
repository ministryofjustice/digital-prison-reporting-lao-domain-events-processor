package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import io.awspring.cloud.sqs.annotation.SqsListener
import org.springframework.stereotype.Service
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.readValue
import uk.gov.justice.hmpps.sqs.SnsMessage
import java.time.ZonedDateTime

@Service
class InboundMessageListener(
  private val laoDataUpdateService: LaoDataUpdateService,
  private val laoCrnInitialisationService: LaoCrnInitialisationService,
  private val jsonMapper: JsonMapper,
) {
  /**
   * Get the LAO event and check to see if it's an addition, removal, or a change in an existing entry.
   * Ensure that there is exactly one change
   */
  @SqsListener("inboundqueue", factory = "hmppsQueueContainerFactoryProxy")
  fun processMessage(message: SnsMessage) {
    val event: LAOEvent = jsonMapper.readValue(message.message)

    val crnIdentifiers = event.personReference.identifiers.filter { it.type == "CRN" }
    if (crnIdentifiers.size != 1) {
      throw IllegalArgumentException("List of identifiers had ${crnIdentifiers.size} CRN identifiers instead of 1")
    }
    if (crnIdentifiers.first().type.lowercase() != "crn") {
      return
    }

    val crn = crnIdentifiers.first().value
    if (crn.isBlank()) {
      throw IllegalArgumentException("CRN value was blank")
    }

    laoCrnInitialisationService.insertCrnIfNeeded(crn)
    laoDataUpdateService.process(crn)
  }
}

data class LAOEvent(
  val eventType: String,
  val version: Int,
  val description: String,
  val occurredAt: ZonedDateTime,
  val personReference: PersonReference,
) {
  data class PersonReference(
    val identifiers: List<Identifier>,
  ) {
    data class Identifier(
      val type: String,
      val value: String,
    )
  }
}

enum class LaoDataType {
  Restriction,
  Exclusion,
}
