package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoCrn
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
  private val laoCrnRepository: LaoCrnRepository,
  private val transactionalRunner: TransactionalRunner,
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

    val liveLaoExclusionsByCrn = allLiveLaoData
      .asSequence()
      .filter { it.type.equals("exclusion", ignoreCase = true) }
      .map {
        LaoExclusion(
          it.crn,
          it.username,
          it.exclusionMessage,
          it.startDate,
          it.endDate,
          "${it.crn}:${it.username}",
        )
      }
      .groupBy { it.crn }
    val liveLaoExclusions = liveLaoExclusionsByCrn.values.flatten()

    val allLocalExclusions = laoExclusionRepository.findAll()
    val allLocalExclusionsHashSet = allLocalExclusions.toHashSet()

    log.info("Finding missing exclusions")
    val missingExclusionsAsHashSet = liveLaoExclusions.filter { it !in allLocalExclusionsHashSet }
    log.info("Got exclusion data")

    val liveLaoRestrictionsByCrn = allLiveLaoData
      .asSequence()
      .filter { it.type.equals("restriction", ignoreCase = true) }
      .map {
        LaoRestriction(
          it.crn,
          it.username,
          it.restrictionMessage,
          it.startDate,
          it.endDate,
          "${it.crn}:${it.username}",
        )
      }
      .groupBy { it.crn }
    val liveLaoRestrictions = liveLaoRestrictionsByCrn.values.flatten()

    val allLocalRestrictions = laoRestrictionRepository.findAll()
    val allLocalRestrictionsHashSet = allLocalRestrictions.toHashSet()

    log.info("Finding missing restrictions")
    val missingRestrictionsAsHashSet = liveLaoRestrictions.filter { it !in allLocalRestrictionsHashSet }
    log.info("Got restriction data")

    val crnsToReconcile = missingRestrictionsAsHashSet.map { it.crn }.plus(missingExclusionsAsHashSet.map { it.crn }).toSet()

    log.info("Processing ${missingExclusionsAsHashSet.size} missing excl")
    log.info("Processing ${missingRestrictionsAsHashSet.size} missing restr")
    log.info("Processing ${crnsToReconcile.size} crns")

    val existingCrns = laoCrnRepository.findAll().map { it.crn }.toHashSet()
    log.info("Found existing crns: ${existingCrns.size}")
    val missingCrns = (liveLaoExclusionsByCrn.keys + liveLaoRestrictionsByCrn.keys).toHashSet() - existingCrns
    log.info("Found missing crns: ${missingCrns.size}")

    if (dryRun == "true") {
      log.info("Doing dryrun, exiting.")
      return
    }

    log.info("adding missing crns")
    missingCrns.chunked(500).forEachIndexed { index, chunk ->
      log.info("Chunk $index of missing crns")
      laoCrnRepository.saveAll(
        chunk.map {
          LaoCrn(
            crn = it,
            version = 0,
          )
        },
      )
    }
    log.info("finished adding missing crns")

    log.info("Starting to reconcile restrictions and exclusions")
    crnsToReconcile.chunked(500).forEachIndexed { idx, crnChunk ->
      log.info("Chunk $idx of crns to reconcile restrictions and exclusions of")
      transactionalRunner.run {
        log.info("Processed index $idx")
        val exclusions = crnChunk.flatMap { liveLaoExclusionsByCrn[it].orEmpty() }
        val restrictions = crnChunk.flatMap { liveLaoRestrictionsByCrn[it].orEmpty() }

        if (exclusions.isNotEmpty()) {
          laoExclusionRepository.deleteAllByCrns(crnChunk)
          laoExclusionRepository.saveAll(exclusions)
        }
        if (restrictions.isNotEmpty()) {
          laoRestrictionRepository.deleteAllByCrns(crnChunk)
          laoRestrictionRepository.saveAll(restrictions)
        }
      }
    }
  }
}
