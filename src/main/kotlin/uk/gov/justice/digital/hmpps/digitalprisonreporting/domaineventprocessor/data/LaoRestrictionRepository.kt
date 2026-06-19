package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.time.ZonedDateTime

@Repository
interface LaoRestrictionRepository : JpaRepository<LaoRestriction, String> {

  @Query("SELECT r FROM LaoRestriction r WHERE r.crn = :crn")
  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction>
}

@Entity
@Table(
  name = "lao_restrictions",
  schema = "product_",
)
class LaoRestriction(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: ZonedDateTime,
  val until: ZonedDateTime?,
  @Id
  val crnUserId: String,
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other !is LaoExclusion) return false

    if (crnUserId != other.crnUserId) return false

    return true
  }

  override fun hashCode(): Int = crnUserId.hashCode()
}
fun LaoRestriction.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
