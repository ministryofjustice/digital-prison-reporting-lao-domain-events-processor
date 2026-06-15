package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.model

import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoExclusion
import uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data.LaoRestriction
import java.time.LocalDateTime

data class LaoEntry(
  val crn: String,
  val userId: String,
  val reason: String?,
  val since: LocalDateTime,
  val until: LocalDateTime?,
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other !is LaoEntry) return false

    return this.crn == other.crn && this.userId == other.userId && this.reason == other.reason && this.since == other.since && this.until == other.until
  }

  override fun hashCode(): Int {
    var result = +crn.hashCode()
    result = 31 * result + userId.hashCode()
    result = 31 * result + (reason?.hashCode() ?: 0)
    result = 31 * result + since.hashCode()
    result = 31 * result + (until?.hashCode() ?: 0)
    return result
  }
}
fun LaoEntry.toExclusion() = LaoExclusion(crn, userId, reason, since, until, null)
fun LaoEntry.toRestriction() = LaoRestriction(crn, userId, reason, since, until, null)
