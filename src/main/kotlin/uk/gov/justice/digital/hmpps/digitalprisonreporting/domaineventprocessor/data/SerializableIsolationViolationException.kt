package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.data

import org.springframework.jdbc.UncategorizedSQLException

class SerializableIsolationViolationException(
  cause: Throwable,
) : RuntimeException(cause)

internal fun Throwable.toRetryableExceptionIfRequired(): RuntimeException? {
  var current: Throwable? = this

  while (current != null) {
    if (
      current is UncategorizedSQLException &&
      current.message?.contains("Invalid operation: 1023") == true
    ) {
      return SerializableIsolationViolationException(this)
    }

    current = current.cause
  }

  return null
}
