package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Id
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.sql.Timestamp
import java.sql.Types
import java.time.ZonedDateTime

@Repository
class LaoRestrictionRepository(
  val jdbcTemplate: JdbcTemplate,
) {
  fun deleteByCrn(crn: String): Int = jdbcTemplate.update(
    """
      DELETE FROM product_.lao_restrictions
      WHERE crn = ?
    """.trimIndent(),
    crn,
  )

  fun saveAll(restrictions: Collection<LaoRestriction>) {
    jdbcTemplate.batchUpdate(
      """
      INSERT INTO product_.lao_restrictions
      (
        crn,
        user_id,
        reason,
        since,
        until,
        crn_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)
      """.trimIndent(),
      restrictions,
      restrictions.size,
    ) { ps, restriction ->
      ps.setString(1, restriction.crn)
      ps.setString(2, restriction.userId)
      ps.setString(3, restriction.reason)
      ps.setTimestamp(4, Timestamp.from(restriction.since.toInstant()))
      if (restriction.until != null) ps.setTimestamp(5, Timestamp.from(restriction.until.toInstant())) else ps.setNull(5, Types.TIMESTAMP)
      ps.setString(6, restriction.crnUserId)
    }
  }
}

data class LaoRestriction(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: ZonedDateTime,
  val until: ZonedDateTime?,
  @Id
  val crnUserId: String,
)
fun LaoRestriction.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
