package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import io.awspring.cloud.sqs.annotation.SqsListener
import org.springframework.stereotype.Service
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.readValue
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoDataType
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoEntry
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoEventRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient
import uk.gov.justice.hmpps.sqs.SnsMessage
import java.util.Date

@Service
class InboundMessageListener(
  private val laoEventRepository: LaoEventRepository,
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val jsonMapper: JsonMapper,
) {

  /**
   * Get the LAO event and check to see if it's an addition, removal, or a change in an existing entry.
   * Ensure that there is exactly one change
   */
  @SqsListener("inboundqueue", factory = "hmppsQueueContainerFactoryProxy")
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
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoEntry(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until) }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoEntry(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until) }

    val localLaoData = laoEventRepository.getLaoDataForCrn(crn)

    if (liveLaoDataTransformedRestrictions.size != localLaoData.restrictions.size && liveLaoDataTransformedExclusions.size != localLaoData.exclusions.size) {
      throw IllegalArgumentException("""
        Restrictions AND exclusions for crn $crn were added and/or removed:
        Live restrictions: ${liveLaoDataTransformedRestrictions.size}
        Local restrictions: ${localLaoData.restrictions.size}
        Live exclusions: ${liveLaoDataTransformedExclusions.size}
        Local exclusions: ${localLaoData.exclusions.size}
      """.trimIndent())
    }
    if (liveLaoDataTransformedRestrictions.size != localLaoData.restrictions.size) {
      processChanges(liveLaoDataTransformedRestrictions, localLaoData.restrictions, LaoDataType.Restriction)
      return
    }

    if (liveLaoDataTransformedExclusions.size != localLaoData.exclusions.size) {
      processChanges(liveLaoDataTransformedExclusions, localLaoData.exclusions, LaoDataType.Exclusion)
      return
    }

    val processedRestrictionsSuccess = processUpdates(liveLaoDataTransformedRestrictions, localLaoData.restrictions, LaoDataType.Restriction)
    val processedExclusionsSuccess = processUpdates(liveLaoDataTransformedExclusions, localLaoData.exclusions, LaoDataType.Exclusion)

    if (processedExclusionsSuccess || processedRestrictionsSuccess) {
      return
    }
    throw IllegalArgumentException("No changes detected even though event was fired!")
  }

  private fun processUpdates(liveEntries: List<LaoEntry>, localEntries: List<LaoEntry>, laoDataType: LaoDataType): Boolean {
    val differencesLocal = localEntries.subtract(liveEntries)
    val differencesLive = liveEntries.subtract(localEntries)

    if (differencesLive.size != 1 || differencesLocal.size != 1) {
      return false
    }
    val liveDiffEntry = differencesLive.first()
    val localDiffEntry = differencesLocal.first()
    if (localDiffEntry.crn != liveDiffEntry.crn || localDiffEntry.user != liveDiffEntry.user) {
      throw IllegalArgumentException("The record changed should be for the same user and CRN. Instead, it was for local user ${"foobar"} and crn $localDiffEntry.crn and live user ${"foobar"} and crn $liveDiffEntry.crn")
    }
    laoEventRepository.updateLaoEntry(liveDiffEntry, laoDataType)
    return true
  }

  private fun processChanges(liveEntries: List<LaoEntry>, localEntries: List<LaoEntry>, laoDataType: LaoDataType) {
    if (liveEntries.size > localEntries.size) {
      val newElementList = liveEntries.subtract(localEntries)
      if (newElementList.size != 1) {
        throw IllegalArgumentException("More than 1 LAO entry changed!")
      }

      laoEventRepository.addLaoEntry(newElementList.first(), laoDataType)
    }

    val removedElementList = localEntries.subtract(liveEntries)
    if (removedElementList.size != 1) {
      throw IllegalArgumentException("More than 1 LAO entry changed!")
    }
    laoEventRepository.deleteLaoEntry(removedElementList.first().crn, removedElementList.first().user, LaoDataType.Exclusion)
  }
}

data class LAOEvent(
  val eventType: String,
  val version: Int,
  val description: String,
  val occurredAt: Date,
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