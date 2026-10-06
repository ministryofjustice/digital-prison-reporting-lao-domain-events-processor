package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Id
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model.LaoEntry
import java.sql.Timestamp
import java.sql.Types
import java.time.ZoneId
import java.time.ZonedDateTime

@Repository
class LaoExclusionRepository(
  val jdbcTemplate: JdbcTemplate,
) {
  private val utcZone = ZoneId.of("Z")
  fun deleteByCrn(crn: String): Int = jdbcTemplate.update(
    """
      DELETE FROM product_.lao_exclusions
      WHERE crn = ?
    """.trimIndent(),
    crn,
  )

  fun deleteAllByCrns(crns: List<String>): Int {
    val crnValues = crns.joinToString(",") { "?" }
    return jdbcTemplate.update(
      """
      DELETE FROM product_.lao_exclusions
      WHERE crn in ($crnValues)
      """.trimIndent(),
      *crns.toTypedArray(),
    )
  }

  fun findAll(): List<LaoExclusion> {
    val sql = """
      SELECT
        crn,
        user_id,
        reason,
        since,
        until,
        crn_user_id
      FROM product_.lao_exclusions
    """.trimIndent()
    return time("Full mapping - exclusions") {
      jdbcTemplate.query(sql) { rs, _ ->
        val until = rs.getTimestamp("until")
        LaoExclusion(
          rs.getString("crn"),
          rs.getString("user_id"),
          rs.getString("reason"),
          ZonedDateTime.ofInstant(rs.getTimestamp("since").toInstant(), utcZone),
          if (until != null) ZonedDateTime.ofInstant(until.toInstant(), utcZone) else null,
          rs.getString("crn_user_id"),
        )
      }
    }
  }

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
      if (exclusion.until != null) ps.setTimestamp(5, Timestamp.from(exclusion.until.toInstant())) else ps.setNull(5, Types.TIMESTAMP)
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
