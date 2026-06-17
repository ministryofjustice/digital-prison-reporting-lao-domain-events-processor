package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.time.ZonedDateTime

@Repository
interface LaoRestrictionRepository : JpaRepository<LaoRestriction, Long> {

  @Query("SELECT r FROM LaoRestriction r WHERE r.crn = :crn")
  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction>

  @Modifying
  @Query("DELETE FROM LaoRestriction r WHERE r.crn = :crn")
  fun deleteAllForCrn(crn: String)
}

@Entity
@Table(
  name = "lao_restrictions",
  schema = "product_",
  uniqueConstraints = [UniqueConstraint(columnNames = ["crn", "userId"])]
)
data class LaoRestriction(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: ZonedDateTime,
  val until: ZonedDateTime?,
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  val id: Long?,
)
fun LaoRestriction.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
