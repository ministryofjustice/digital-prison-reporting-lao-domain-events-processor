package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.time.LocalDateTime

@Repository
interface LaoExclusionRepository : JpaRepository<LaoExclusion, Long> {
  @Query("SELECT e FROM LaoExclusion e WHERE e.crn = :crn")
  fun getLaoExclusionsForCrn(crn: String): List<LaoExclusion>

  @Modifying
  @Query("DELETE FROM LaoExclusion e WHERE e.crn = :crn AND e.userId = :userId")
  fun deleteExclusionLaoEntry(crn: String, userId: String)

  @Modifying
  @Query("UPDATE LaoExclusion e SET e.crn = :#{#laoEntry.crn}, e.userId = :#{#laoEntry.userId}, e.reason = :#{#laoEntry.reason}, e.since = :#{#laoEntry.since}, e.until = :#{#laoEntry.until} WHERE e.crn = :#{#laoEntry.crn}")
  fun updateExclusionLaoEntry(laoEntry: LaoEntry)
}

@Entity
@Table(name = "lao_exclusions", schema = "product_")
data class LaoExclusion(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: LocalDateTime,
  val until: LocalDateTime?,
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  val id: Long?,
)
fun LaoExclusion.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
