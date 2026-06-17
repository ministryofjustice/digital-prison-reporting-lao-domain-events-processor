package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import io.awspring.cloud.sqs.annotation.SqsListener
import jakarta.transaction.Transactional
import org.springframework.stereotype.Service
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.readValue
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestrictionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient
import uk.gov.justice.hmpps.sqs.SnsMessage
import java.time.LocalDateTime

@Service
class InboundMessageListener(
  private val laoExclusionRepository: LaoExclusionRepository,
  private val laoRestrictionRepository: LaoRestrictionRepository,
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val jsonMapper: JsonMapper,
) {
  /**
   * Get the LAO event and check to see if it's an addition, removal, or a change in an existing entry.
   * Ensure that there is exactly one change
   */
  @SqsListener("inboundqueue", factory = "hmppsQueueContainerFactoryProxy")
  @Transactional
  fun processMessage(message: SnsMessage) {
    val event: LAOEvent = jsonMapper.readValue(message.message)
    val identifiers = event.personReference.identifiers
    if (identifiers.size != 1) {
      throw IllegalArgumentException("List of identifiers had ${identifiers.size} length instead of 1")
    }
    if (identifiers.first().type.lowercase() != "crn") {
      return
    }
    val crn = identifiers.first().value
    if (crn.isBlank()) {
      throw IllegalArgumentException("CRN value was blank")
    }
    val liveLaoData = laoDataProbationIntegrationClient.getLaoData(crn)
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoExclusion(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until, null) }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoRestriction(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until, null) }

    laoExclusionRepository.deleteAllForCrn(crn)
    laoRestrictionRepository.deleteAllForCrn(crn)
    laoExclusionRepository.saveAll(liveLaoDataTransformedExclusions)
    laoRestrictionRepository.saveAll(liveLaoDataTransformedRestrictions)
  }
}

data class LAOEvent(
  val eventType: String,
  val version: Int,
  val description: String,
  val occurredAt: LocalDateTime,
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

data class LaoData(
  val exclusions: List<LaoEntry>,
  val restrictions: List<LaoEntry>,
)

enum class LaoDataType {
  Restriction,
  Exclusion,
}
