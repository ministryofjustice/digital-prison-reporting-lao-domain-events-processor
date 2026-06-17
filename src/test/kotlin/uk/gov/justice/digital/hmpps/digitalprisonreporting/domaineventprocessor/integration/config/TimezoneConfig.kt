package uk.gov.justice.digital.hmpps.digitalprisonreporting.domaineventprocessor.integration.config

import org.springframework.beans.BeansException
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.test.context.TestComponent
import java.util.*


@TestComponent
class GlobalTimezoneBeanFactoryPostProcessor : BeanFactoryPostProcessor {
  @Throws(BeansException::class)
  override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+01:00"))
  }
}