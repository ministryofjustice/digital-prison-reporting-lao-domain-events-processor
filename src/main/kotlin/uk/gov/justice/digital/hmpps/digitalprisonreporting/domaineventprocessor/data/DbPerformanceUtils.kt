package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

fun <T> time(label: String, block: () -> T): T {
  val start = System.currentTimeMillis()
  return block().also {
    println("$label took ${System.currentTimeMillis() - start}ms")
  }
}
