package io.kafbat.ui.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.session.WebSessionManager;
import reactor.test.StepVerifier;

class ConfigTest {

  private final Config config = new Config(null, null);

  @Test
  void usesDefaultThirtyMinuteSessionTimeoutWhenNotConfigured() {
    WebSessionManager manager = config.webSessionManager(new AuthProperties());
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/"));

    StepVerifier.create(manager.getSession(exchange))
        .assertNext(session ->
            assertThat(session.getMaxIdleTime()).isEqualTo(Duration.ofMinutes(30)))
        .verifyComplete();
  }

  @Test
  void appliesConfiguredSessionTimeout() {
    AuthProperties authProperties = new AuthProperties();
    authProperties.setSessionTimeout(Duration.ofHours(2));

    WebSessionManager manager = config.webSessionManager(authProperties);
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/"));

    StepVerifier.create(manager.getSession(exchange))
        .assertNext(session ->
            assertThat(session.getMaxIdleTime()).isEqualTo(Duration.ofHours(2)))
        .verifyComplete();
  }
}
