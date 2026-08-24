package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
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
}
