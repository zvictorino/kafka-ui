package io.kafbat.ui.config;

import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("auth")
public class AuthProperties {

  private Duration sessionTimeout = Duration.ofMinutes(30);

}
