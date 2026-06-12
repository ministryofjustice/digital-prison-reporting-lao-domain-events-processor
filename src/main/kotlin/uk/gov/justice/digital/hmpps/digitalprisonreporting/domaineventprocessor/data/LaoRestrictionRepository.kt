package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.jpa.repository.QueryRewriter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

//@Service
@Repository
interface LaoRestrictionRepository:
//  (val dataSource: DataSource,):
  JpaRepository<LaoRestriction, Long>{

  @Query("SELECT r FROM LaoRestriction r WHERE r.crn = :crn")
  fun getLaoRestrictionsForCrn(crn: String): List<LaoRestriction>

  @Modifying
  @Query("DELETE FROM LaoRestriction r WHERE r.crn = :crn AND r.userId = :userId")
  fun deleteRestrictionLaoEntry(crn: String, userId: String)

  @Modifying
  @Query("UPDATE LaoRestriction r SET r.crn = :#{#laoEntry.crn}, r.userId = :#{#laoEntry.userId}, r.reason = :#{#laoEntry.reason}, r.since = :#{#laoEntry.since}, r.until = :#{#laoEntry.until} WHERE r.crn = :#{#laoEntry.crn}")
  fun updateRestrictionLaoEntry(laoEntry: LaoEntry)
//  fun populateNamedParameterJdbcTemplate(): JdbcTemplate = JdbcTemplate(dataSource)
//
//  fun getLaoDataForCrn(crn: String): LaoData {
//    val jdbcTemplate = populateNamedParameterJdbcTemplate()
//    return LaoData(
//      jdbcTemplate.queryForList(
//        "SELECT crn, user_id, reason, since, until FROM product_.lao_exclusions WHERE crn = ?",
//        LaoEntry::class.java,
//        crn,
//      ) as List<LaoEntry>,
//      jdbcTemplate.queryForList(
//        "SELECT crn, user_id, reason, since, until FROM product_.lao_restrictions WHERE crn = ?",
//        LaoEntry::class.java,
//        crn,
//      ) as List<LaoEntry>,
//    )
//  }
//
//  fun deleteLaoEntry(crn: String, userId: String, dataType: LaoDataType) {
//    val jdbcTemplate = populateNamedParameterJdbcTemplate()
//    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
//    jdbcTemplate.execute("DELETE from product_.$table WHERE crn = '$crn' and user_id = '$userId'")
//  }
//
//  fun addLaoEntry(laoEntry: LaoEntry, dataType: LaoDataType) {
//    val jdbcTemplate = populateNamedParameterJdbcTemplate()
//    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
//    jdbcTemplate.execute("INSERT INTO product_.$table (crn, user_id, reason, since, until) VALUES ('${laoEntry.crn}', '${laoEntry.userId}', '${laoEntry.reason}', '${laoEntry.since}', '${laoEntry.until ?: "NULL"}')")
//  }
//
//  fun updateLaoEntry(laoEntry: LaoEntry, dataType: LaoDataType) {
//    val jdbcTemplate = populateNamedParameterJdbcTemplate()
//    val table = if (dataType == LaoDataType.Exclusion) "lao_exclusions" else "lao_restrictions"
//    jdbcTemplate.execute("UPDATE product_.$table SET reason = '${laoEntry.reason}', since = '${laoEntry.since}', until = '${laoEntry.until ?: "NULL"}' WHERE crn = '${laoEntry.crn}' AND user_id = '${laoEntry.userId}'")
//  }
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
fun LaoRestriction.toLaoEntry() = LaoEntry(crn, userId, reason, since, until, id)

