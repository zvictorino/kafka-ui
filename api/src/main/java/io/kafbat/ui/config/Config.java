package io.kafbat.ui.config;

import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.openapitools.jackson.nullable.JsonNullableModule;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.autoconfigure.web.reactive.WebFluxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ContextPathCompositeHandler;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.util.StringUtils;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import org.springframework.web.server.session.DefaultWebSessionManager;
import org.springframework.web.server.session.WebSessionManager;

@Configuration
@AllArgsConstructor
@EnableConfigurationProperties(AuthProperties.class)
public class Config {

  private final ApplicationContext applicationContext;

  private final ServerProperties serverProperties;

  @Bean
  public HttpHandler httpHandler(ObjectProvider<WebFluxProperties> propsProvider) {

    final String basePath = serverProperties.getServlet().getContextPath();

    HttpHandler httpHandler = WebHttpHandlerBuilder
        .applicationContext(this.applicationContext).build();

    if (StringUtils.hasText(basePath)) {
      Map<String, HttpHandler> handlersMap =
          Collections.singletonMap(basePath, httpHandler);
      return new ContextPathCompositeHandler(handlersMap);
    }
    return httpHandler;
  }

  @Bean(WebHttpHandlerBuilder.WEB_SESSION_MANAGER_BEAN_NAME)
  public WebSessionManager webSessionManager(AuthProperties authProperties) {
    var delegate = new DefaultWebSessionManager();
    return exchange -> delegate.getSession(exchange)
        .doOnNext(session -> session.setMaxIdleTime(authProperties.getSessionTimeout()));
  }

  @Bean
  public MBeanExporter exporter() {
    final var exporter = new MBeanExporter();
    exporter.setAutodetect(true);
    exporter.setExcludedBeans("pool");
    return exporter;
  }

  @Bean
  // will be used by webflux json mapping
  public JsonNullableModule jsonNullableModule() {
    return new JsonNullableModule();
  }
}
