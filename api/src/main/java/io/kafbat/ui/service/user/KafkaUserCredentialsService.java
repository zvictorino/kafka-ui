package io.kafbat.ui.service.user;

import io.kafbat.ui.model.KafkaCluster;
import java.util.Collection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface KafkaUserCredentialsService {
  Flux<KafkaUser> listUsers(KafkaCluster cluster);

  Mono<Void> upsertUser(KafkaCluster cluster, String username, String password, Collection<String> mechanisms);

  Mono<Void> deleteCredential(KafkaCluster cluster, String username, String mechanism);

  Mono<Void> deleteUserCredentials(KafkaCluster cluster, String username);
}
