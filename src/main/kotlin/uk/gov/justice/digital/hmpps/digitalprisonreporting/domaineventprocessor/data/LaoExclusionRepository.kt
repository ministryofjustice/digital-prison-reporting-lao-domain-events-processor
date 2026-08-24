package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Id
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.time.OffsetDateTime
import java.time.ZonedDateTime

@Repository
class LaoExclusionRepository(
  private val jdbcTemplate: JdbcTemplate,
) {

  fun getLaoExclusionsForCrn(crn: String): List<LaoExclusion> = jdbcTemplate.query(
    """
      SELECT
        crn,
        user_id,
        reason,
        since,
        until,
        crn_user_id
      FROM product_.lao_exclusions
      WHERE crn = ?
    """.trimIndent(),
    { rs, _ ->
      LaoExclusion(
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
      DELETE FROM product_.lao_exclusions
      WHERE crn = ?
    """.trimIndent(),
    crn,
  )

  fun saveAll(exclusions: Collection<LaoExclusion>) {
    jdbcTemplate.batchUpdate(
      """
      INSERT INTO product_.lao_exclusions
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
      exclusions,
      exclusions.size,
    ) { ps, exclusion ->
      ps.setString(1, exclusion.crn)
      ps.setString(2, exclusion.userId)
      ps.setString(3, exclusion.reason)
      ps.setObject(4, exclusion.since.toOffsetDateTime())
      ps.setObject(5, exclusion.until?.toOffsetDateTime())
      ps.setString(6, exclusion.crnUserId)
    }
  }
}

data class LaoExclusion(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: ZonedDateTime,
  val until: ZonedDateTime?,
  @Id
  val crnUserId: String,
)
fun LaoExclusion.toLaoEntry() = LaoEntry(crn, userId, reason, since, until)
