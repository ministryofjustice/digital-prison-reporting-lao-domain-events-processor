package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.CascadeType
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.OneToMany
import jakarta.persistence.PostLoad
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.hibernate.annotations.JdbcType
import org.hibernate.type.descriptor.jdbc.VarcharJdbcType
import org.springframework.data.domain.Persistable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.util.UUID

@Repository
interface LaoCrnRepository : JpaRepository<LaoCrn, UUID> {
  fun findByCrn(crn: String): Collection<LaoCrn>
}

@Entity
@Table(name = "lao_crns", schema = "product_")
class LaoCrn(
  @Id
  @Column(name = "id", columnDefinition = "TEXT")
  @JdbcType(value = VarcharJdbcType::class)
  val uuid: UUID? = UUID.randomUUID(),

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
) : Persistable<UUID> {
  @Transient
  var new: Boolean = true
  override fun getId(): UUID? = uuid

  override fun isNew() = new

  @PostLoad
  fun onLoad() {
    new = false
  }

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
