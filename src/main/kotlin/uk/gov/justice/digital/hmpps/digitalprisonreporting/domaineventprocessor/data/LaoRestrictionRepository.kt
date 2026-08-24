package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Id
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.time.OffsetDateTime
import java.time.ZonedDateTime

@Repository
class LaoRestrictionRepository(
  private val jdbcTemplate: JdbcTemplate,
) {

  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction> = jdbcTemplate.query(
    """
      SELECT
        crn,
        user_id,
        reason,
        since,
        until,
        crn_user_id
      FROM product_.lao_restrictions
      WHERE crn = ?
    """.trimIndent(),
    { rs, _ ->
      LaoRestriction(
        crn = rs.getString("crn"),
        userId = rs.getString("user_id"),
        reason = rs.getString("reason"),
        since = rs.getObject("since", OffsetDateTime::class.java).toZonedDateTime(),
        until = rs.getObject("until", OffsetDateTime::class.java)?.toZonedDateTime(),
        crnUserId = rs.getString("crn_user_id"),
      )
    },
    crn,
  )

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
      ps.setObject(4, restriction.since.toOffsetDateTime())
      ps.setObject(5, restriction.until?.toOffsetDateTime())
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
