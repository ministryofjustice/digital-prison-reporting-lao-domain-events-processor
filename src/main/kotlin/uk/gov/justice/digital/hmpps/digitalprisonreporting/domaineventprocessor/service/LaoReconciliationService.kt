package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrnRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestrictionRepository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.probationintegration.LaoDataProbationIntegrationClient

@Service
class LaoReconciliationService(
  private val laoDataProbationIntegrationClient: LaoDataProbationIntegrationClient,
  private val laoCrnInitialisationService: LaoCrnInitialisationService,
  private val laoDataUpdateService: LaoDataUpdateService,
  private val transactionalRunner: TransactionalRunner,
  private val laoCrnRepository: LaoCrnRepository,
  private val laoExclusionRepository: LaoExclusionRepository,
  private val laoRestrictionRepository: LaoRestrictionRepository,
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  fun reconcile() {
    val allLiveLaoData = laoDataProbationIntegrationClient.getAllLaoData()

    val allLocalExclusions = laoExclusionRepository.findAll()
    val liveExclusionsByCrn = allLiveLaoData.filter { it.type.lowercase() == "exclusion" }.groupBy { it.crn }
    val missingExclusions = liveExclusionsByCrn.filter {
      val liveLaoExclusionsForCrn = it.value.map { entry ->
        LaoExclusion(
          it.key,
          entry.username,
          entry.exclusionMessage,
          entry.startDate,
          entry.endDate,
          "${it.key}:${entry.username}",
        )
      }
      !allLocalExclusions.containsAll(liveLaoExclusionsForCrn)
    }

    val allLocalRestrictions = laoRestrictionRepository.findAll()
    val liveRestrictionsByCrn = allLiveLaoData.filter { it.type.lowercase() == "restriction" }.groupBy { it.crn }
    val missingRestrictions = liveRestrictionsByCrn.filter {
      val liveLaoRestrictionsForCrn = it.value.map { entry ->
        LaoRestriction(
          it.key,
          entry.username,
          entry.exclusionMessage,
          entry.startDate,
          entry.endDate,
          "${it.key}:${entry.username}",
        )
      }
      !allLocalRestrictions.containsAll(liveLaoRestrictionsForCrn)
    }

    log.info("Processing ${missingExclusions.keys.size} missing excl")
    log.info("Processing ${missingRestrictions.keys.size} missing restr")

    val crnsToReconcile = missingRestrictions.keys.plus(missingExclusions.keys).toSet()
    transactionalRunner.run {
      crnsToReconcile.forEachIndexed { idx, it ->
        if (idx % 10 == 0) {
          log.info("Processed index $idx")
        }
        laoCrnInitialisationService.insertCrnIfNeeded(it)
        // We need to re-fetch these because it's possible a crn only had restrictions or only exclusions change, in which case the filtering
        // we did previously may filter out one side of these from the missingRestrictions/missingExclusions
        val laoExclusions = liveExclusionsByCrn[it]
          ?.filter { it.type.lowercase() == "exclusion" }
          ?.map {
            LaoExclusion(
              it.crn,
              it.username,
              it.exclusionMessage,
              it.startDate,
              it.endDate,
              "${it.crn}:${it.username}",
            )
          }
          .orEmpty()
        val laoRestrictions = liveRestrictionsByCrn[it]
          ?.filter { it.type.lowercase() == "restriction" }
          ?.map {
            LaoRestriction(
              it.crn,
              it.username,
              it.restrictionMessage,
              it.startDate,
              it.endDate,
              "${it.crn}:${it.username}",
            )
          }
          .orEmpty()
        laoDataUpdateService.saveLaoDataForCrn(it, laoExclusions, laoRestrictions)
      }
    }
  }
}
