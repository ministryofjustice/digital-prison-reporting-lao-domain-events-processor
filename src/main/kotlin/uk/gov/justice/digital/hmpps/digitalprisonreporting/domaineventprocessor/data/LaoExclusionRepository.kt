package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.jpa.repository.QueryRewriter
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

//@Service
@Repository
interface LaoExclusionRepository:
//  (val dataSource: DataSource,):
  JpaRepository<LaoExclusion, Long> {
  @Query("SELECT e FROM LaoExclusion e WHERE e.crn = :crn")
  fun getLaoExclusionsForCrn(crn: String): List<LaoExclusion>

  @Modifying
  @Query("DELETE FROM LaoExclusion e WHERE e.crn = :crn AND e.userId = :userId")
  fun deleteExclusionLaoEntry(crn: String, userId: String)

  @Modifying
  @Query("UPDATE LaoExclusion e SET e.crn = :#{#laoEntry.crn}, e.userId = :#{#laoEntry.userId}, e.reason = :#{#laoEntry.reason}, e.since = :#{#laoEntry.since}, e.until = :#{#laoEntry.until} WHERE e.crn = :#{#laoEntry.crn}")
  fun updateExclusionLaoEntry(laoEntry: LaoEntry)

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
  val userId: String,
  val reason: String?,
  val since: LocalDateTime,
  val until: LocalDateTime?,
  val id: Long?,
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other !is LaoEntry) return false

    return this.crn == other.crn && this.userId == other.userId && this.reason == other.reason && this.since == other.since && this.until == other.until
  }

  override fun hashCode(): Int {
    var result = id?.hashCode() ?: 0
    result = 31 * result + crn.hashCode()
    result = 31 * result + userId.hashCode()
    result = 31 * result + (reason?.hashCode() ?: 0)
    result = 31 * result + since.hashCode()
    result = 31 * result + (until?.hashCode() ?: 0)
    return result
  }
}
fun LaoEntry.toExclusion() = LaoExclusion(crn,userId,reason,since,until,id)
fun LaoEntry.toRestriction() = LaoRestriction(crn,userId,reason,since,until,id)

@Entity
@Table(name = "lao_exclusions", schema = "product_")
data class LaoExclusion(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: LocalDateTime,
  val until: LocalDateTime?,
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  val id: Long?,
)
fun LaoExclusion.toLaoEntry() = LaoEntry(crn, userId, reason, since, until, id)
