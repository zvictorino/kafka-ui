package io.kafbat.ui.service.user;

import java.util.Set;
import org.apache.kafka.clients.admin.ScramMechanism;

public record KafkaUser(String username, Set<ScramMechanism> mechanisms) {
}
