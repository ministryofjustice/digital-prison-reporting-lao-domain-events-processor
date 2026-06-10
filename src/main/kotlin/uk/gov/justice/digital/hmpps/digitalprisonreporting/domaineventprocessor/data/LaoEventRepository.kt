package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.util.*
import javax.sql.DataSource

@Service
class LaoEventRepository(
  val dataSource: DataSource,
) {
  fun populateNamedParameterJdbcTemplate(): JdbcTemplate = JdbcTemplate(dataSource)

  fun getLaoDataForCrn(crn: String): LaoData {
    val jdbcTemplate = populateNamedParameterJdbcTemplate()
    return LaoData(
      jdbcTemplate.queryForList(
        "SELECT crn, user, reason, since, until FROM product_.lao_exclusions WHERE crn = $crn",
        LaoEntry::class.java,
      ) as List<LaoEntry>,
      jdbcTemplate.queryForList(
        "SELECT crn, user, reason, since, until FROM product_.lao_restrictions WHERE crn = $crn",
        LaoEntry::class.java,
      ) as List<LaoEntry>,
    )
  }

  fun deleteLaoEntry(crn: String, userId: String, dataType: LaoDataType) {
    val jdbcTemplate = populateNamedParameterJdbcTemplate()
    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
    jdbcTemplate.execute("DELETE from $table WHERE crn_id = $crn and user_id = $userId")
  }

  fun addLaoEntry(laoEntry: LaoEntry, dataType: LaoDataType) {
    val jdbcTemplate = populateNamedParameterJdbcTemplate()
    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
    jdbcTemplate.execute("INSERT INTO $table (crn, user_id, reason, since, until) VALUES (${laoEntry.crn}, ${laoEntry.user}, ${laoEntry.reason}, ${laoEntry.since}, ${laoEntry.until ?: "NULL"})")
  }

  fun updateLaoEntry(laoEntry: LaoEntry, dataType: LaoDataType) {
    val jdbcTemplate = populateNamedParameterJdbcTemplate()
    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
    jdbcTemplate.execute("UPDATE $table SET reason = ${laoEntry.reason}, since = ${laoEntry.since}, until = ${laoEntry.until ?: "NULL"} WHERE crn = ${laoEntry.crn} AND user_id = ${laoEntry.user}")
  }
}

enum class LaoDataType {
  Restriction,
  Exclusion,
}

data class LaoData(
  val exclusions: List<LaoEntry>,
  val restrictions: List<LaoEntry>,
)

data class LaoEntry(
  val crn: String,
  val user: String,
  val reason: String?,
  val since: Date,
  val until: Date?,
)
