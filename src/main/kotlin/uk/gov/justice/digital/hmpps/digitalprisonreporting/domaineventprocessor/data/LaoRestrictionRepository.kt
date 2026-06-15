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
interface LaoRestrictionRepository : JpaRepository<LaoRestriction, Long> {

  @Query("SELECT r FROM LaoRestriction r WHERE r.crn = :crn")
  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction>

  @Modifying
  @Query("DELETE FROM LaoRestriction r WHERE r.crn = :crn AND r.userId = :userId")
  fun deleteRestrictionLaoEntry(crn: String, userId: String)

  @Modifying
  @Query("UPDATE LaoRestriction r SET r.crn = :#{#laoEntry.crn}, r.userId = :#{#laoEntry.userId}, r.reason = :#{#laoEntry.reason}, r.since = :#{#laoEntry.since}, r.until = :#{#laoEntry.until} WHERE r.crn = :#{#laoEntry.crn}")
  fun updateRestrictionLaoEntry(laoEntry: LaoEntry)
}

@Entity
@Table(name = "lao_restrictions", schema = "product_")
data class LaoRestriction(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: LocalDateTime,
  val until: LocalDateTime?,
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  val id: Long?,
)
fun LaoRestriction.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
