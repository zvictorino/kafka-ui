package io.kafbat.ui.service;

import io.kafbat.ui.exception.ValidationException;
import java.util.Locale;
import org.apache.kafka.clients.admin.ScramMechanism;

/**
 * Shared helpers for the SCRAM mechanisms exposed by Kafka user management, so the Admin API and
 * ZooKeeper backends cannot drift apart on mechanism names or iteration counts.
 */
public final class ScramMechanisms {

  private ScramMechanisms() {
  }

  /**
   * Converts an externally visible mechanism name (e.g. SCRAM-SHA-512) into the Admin API enum.
   */
  public static ScramMechanism parse(String value) {
    try {
      return ScramMechanism.valueOf(value.replace('-', '_').toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new ValidationException("Unsupported SCRAM mechanism: " + value, e);
    }
  }

  /**
   * Delegates to Kafka's own minimum rather than hard-coding it, so credentials written by the UI
   * always match what brokers accept. {@code forMechanismName} returns null for unknown mechanisms.
   */
  public static int minimumIterations(ScramMechanism mechanism) {
    var internal = org.apache.kafka.common.security.scram.internals.ScramMechanism
        .forMechanismName(mechanism.mechanismName());
    if (internal == null) {
      throw new ValidationException("Unsupported SCRAM mechanism: " + mechanism);
    }
    return internal.minIterations();
  }
}
