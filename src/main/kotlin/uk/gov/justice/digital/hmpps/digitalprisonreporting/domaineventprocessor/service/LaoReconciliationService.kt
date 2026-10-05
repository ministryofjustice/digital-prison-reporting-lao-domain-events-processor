package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
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
  @Value("batch.dryrun")
  private val dryRun: String = "false",
) {
  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }

  fun reconcile() {
    val allLiveLaoData = laoDataProbationIntegrationClient.getAllLaoData()

    val allLocalExclusions = laoExclusionRepository.findAll()
    val liveExclusionsByCrn = allLiveLaoData.filter { it.type.lowercase() == "exclusion" }.groupBy { it.crn }
    val liveLaoExclusions = liveExclusionsByCrn.values.flatten().map {
      LaoExclusion(
        it.crn,
        it.username,
        it.exclusionMessage,
        it.startDate,
        it.endDate,
        "${it.crn}:${it.username}",
      )
    }
    val missingExclusions = liveLaoExclusions.minus(allLocalExclusions)
    log.info("Got exclusion data")

    val allLocalRestrictions = laoRestrictionRepository.findAll()
    log.info("Got local restrictions")
    val filteredLiveRestrictions = allLiveLaoData.filter { it.type.lowercase() == "restriction" }
    log.info("Filtered live restrictions")
    val liveRestrictionsByCrn = filteredLiveRestrictions.groupBy { it.crn }
    log.info("Grouped live restrictions")
    val flattenedLiveRestrictions = liveRestrictionsByCrn.values.flatten()
    log.info("Flattened live restrictions")
    val liveLaoRestrictions = flattenedLiveRestrictions.map {
      LaoRestriction(
        it.crn,
        it.username,
        it.restrictionMessage,
        it.startDate,
        it.endDate,
        "${it.crn}:${it.username}",
      )
    }
    log.info("Mapped live restrictions")

    val localSet = allLocalRestrictions.toHashSet()

    val start = System.currentTimeMillis()

    val missingRestrictionsAsHashSet =
      liveLaoRestrictions.filter { it !in localSet }

    log.info("diff took {}ms", System.currentTimeMillis() - start)

    log.info("starting minus, ${missingRestrictionsAsHashSet.size} was size of missing restrictions")
    val missingRestrictions = liveLaoRestrictions.minus(allLocalRestrictions)
    log.info("finished diffing")

    log.info("Got restriction data")

    val crnsToReconcile = missingRestrictions.map { it.crn }.plus(missingExclusions.map { it.crn }).toSet()

    log.info("Processing ${missingExclusions.size} missing excl")
    log.info("Processing ${missingRestrictions.size} missing restr")
    log.info("Processing ${crnsToReconcile.size} crns")

    if (dryRun == "true") {
      log.info("Doing dryrun, exiting.")
      return
    }
    transactionalRunner.run {
      crnsToReconcile.forEachIndexed { idx, crn ->
        if (idx % 10 == 0) {
          log.info("Processed index $idx")
        }
        laoCrnInitialisationService.insertCrnIfNeeded(crn)

        val laoExclusions = liveLaoExclusions.filter { it.crn == crn }
        val laoRestrictions = liveLaoRestrictions.filter { it.crn == crn }
        laoDataUpdateService.saveLaoDataForCrn(crn, laoExclusions, laoRestrictions)
      }
    }
  }
}
