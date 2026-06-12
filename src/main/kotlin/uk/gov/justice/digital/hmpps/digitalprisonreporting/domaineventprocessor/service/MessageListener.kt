package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import io.awspring.cloud.sqs.annotation.SqsListener
import jakarta.transaction.Transactional
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.readValue
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.*
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient
import uk.gov.justice.hmpps.sqs.SnsMessage
import java.time.LocalDateTime

@Service
class InboundMessageListener(
  private val applicationContext: ApplicationContext,
  private val laoExclusionRepository: LaoExclusionRepository,
  private val laoRestrictionRepository: LaoRestrictionRepository,
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val jsonMapper: JsonMapper,
) {
  fun getLaoDataForCrn(crn: String): LaoData = LaoData(
    laoExclusionRepository.getLaoExclusionsForCrn(crn).map { it.toLaoEntry() },
    laoRestrictionRepository.getLaoRestrictionsForCrn(crn).map { it.toLaoEntry() },
  )
  fun deleteLaoEntry(crn: String, userId: String, laoDataType: LaoDataType) {
    if (laoDataType == LaoDataType.Exclusion) laoExclusionRepository.deleteExclusionLaoEntry(crn, userId) else laoRestrictionRepository.deleteRestrictionLaoEntry(crn, userId)
  }
  fun addLaoEntry(laoEntry: LaoEntry, laoDataType: LaoDataType) {
    if (laoDataType == LaoDataType.Exclusion) laoExclusionRepository.save(laoEntry.toExclusion()) else laoRestrictionRepository.save(laoEntry.toRestriction())
  }
  fun updateLaoEntry(laoEntry: LaoEntry, laoDataType: LaoDataType) {
    if (laoDataType == LaoDataType.Exclusion) laoExclusionRepository.updateExclusionLaoEntry(laoEntry) else laoRestrictionRepository.updateRestrictionLaoEntry(laoEntry)
  }


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
    val liveLaoDataTransformedExclusions = liveLaoData.excludedFrom.map { LaoEntry(crn, it.username, liveLaoData.exclusionMessage, it.since, it.until, null) }
    val liveLaoDataTransformedRestrictions = liveLaoData.restrictedTo.map { LaoEntry(crn, it.username, liveLaoData.restrictionMessage, it.since, it.until, null) }

    val localLaoData = getLaoDataForCrn(crn)

    if (liveLaoDataTransformedRestrictions.size != localLaoData.restrictions.size && liveLaoDataTransformedExclusions.size != localLaoData.exclusions.size) {
      throw IllegalArgumentException(
        """
        Restrictions AND exclusions for crn $crn were added and/or removed:
        Live restrictions: ${liveLaoDataTransformedRestrictions.size}
        Local restrictions: ${localLaoData.restrictions.size}
        Live exclusions: ${liveLaoDataTransformedExclusions.size}
        Local exclusions: ${localLaoData.exclusions.size}
        """.trimIndent(),
      )
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
    if (localDiffEntry.crn != liveDiffEntry.crn || localDiffEntry.userId != liveDiffEntry.userId) {
      throw IllegalArgumentException("The record changed should be for the same user and CRN. Instead, it was for local user ${"foobar"} and crn $localDiffEntry.crn and live user ${"foobar"} and crn $liveDiffEntry.crn")
    }
    updateLaoEntry(liveDiffEntry, laoDataType)
    return true
  }

  private fun processChanges(liveEntries: List<LaoEntry>, localEntries: List<LaoEntry>, laoDataType: LaoDataType) {
    val newElementList = liveEntries.subtract(localEntries)
    val removedElementList = localEntries.subtract(liveEntries)

    if (newElementList.size + removedElementList.size != 1) {
      throw IllegalArgumentException("Invalid number of LAO entries changed: there were ${newElementList.size} new entries and ${removedElementList.size} removed entries")
    }
    if (liveEntries.size > localEntries.size) {
      addLaoEntry(newElementList.first(), laoDataType)
      return
    }
    deleteLaoEntry(removedElementList.first().crn, removedElementList.first().userId, laoDataType)
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
