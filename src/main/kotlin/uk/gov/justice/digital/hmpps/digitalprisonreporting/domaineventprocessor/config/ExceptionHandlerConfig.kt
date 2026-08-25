package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.config

import org.springframework.context.annotation.Configuration
import org.springframework.retry.annotation.EnableRetry

@EnableRetry
@Configuration
class ExceptionHandlerConfig
