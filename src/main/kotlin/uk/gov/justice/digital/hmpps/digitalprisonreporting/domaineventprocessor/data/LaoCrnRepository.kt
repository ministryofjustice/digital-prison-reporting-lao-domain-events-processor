package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.CascadeType
import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.OneToMany
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

@Repository
interface LaoCrnRepository : JpaRepository<LaoCrn, Long> {
  fun findByCrn(crn: String): Collection<LaoCrn>
}

@Entity
@Table(name = "lao_crns", schema = "product_")
class LaoCrn(
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  val id: Long?,

  val crn: String,

  @Version
  val version: Int,

  @OneToMany(cascade = [CascadeType.ALL], fetch = FetchType.EAGER)
  @JoinColumn(name = "crn", referencedColumnName = "crn")
  val laoExclusions: MutableSet<LaoExclusion>,

  @OneToMany(cascade = [CascadeType.ALL], fetch = FetchType.EAGER)
  @JoinColumn(name = "crn", referencedColumnName = "crn")
  val laoRestrictions: MutableSet<LaoRestriction>,

  var lastUpdated: LocalDateTime = LocalDateTime.now(),
) {
  fun addExclusions(laoExclusions: Collection<LaoExclusion>) {
    this.laoExclusions.clear()
    this.laoExclusions.addAll(laoExclusions)
    lastUpdated = LocalDateTime.now()
  }

  fun addRestrictions(laoRestrictions: Collection<LaoRestriction>) {
    this.laoRestrictions.clear()
    this.laoRestrictions.addAll(laoRestrictions)
    lastUpdated = LocalDateTime.now()
  }
}
