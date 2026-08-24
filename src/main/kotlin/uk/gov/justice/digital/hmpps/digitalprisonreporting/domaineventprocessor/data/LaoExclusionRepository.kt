package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Id
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.sql.Timestamp
import java.sql.Types
import java.time.ZonedDateTime

@Repository
class LaoExclusionRepository(
  val jdbcTemplate: JdbcTemplate,
) {
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
      ps.setTimestamp(4, Timestamp.from(exclusion.since.toInstant()))
      if (exclusion.until != null) ps.setTimestamp(5, Timestamp.from(exclusion.until.toInstant())) else ps.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE)
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
