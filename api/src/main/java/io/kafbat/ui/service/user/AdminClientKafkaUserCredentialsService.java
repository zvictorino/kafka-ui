package io.kafbat.ui.service.user;

import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ScramMechanisms;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Service
public class AdminClientKafkaUserCredentialsService implements KafkaUserCredentialsService {
  private final AdminClientService adminClientService;

  @Override
  public Flux<KafkaUser> listUsers(KafkaCluster cluster) {
    return adminClientService.get(cluster)
        .flatMapMany(client -> client.describeUserScramCredentials()
            .flatMapMany(users -> Flux.fromIterable(users.values())
                .map(user -> new KafkaUser(user.name(), user.credentialInfos().stream()
                    .map(info -> info.mechanism())
                    .collect(java.util.stream.Collectors.toSet())))));
  }

  @Override
  public Mono<Void> upsertUser(KafkaCluster cluster, String username, String password, Collection<String> mechanisms) {
    return adminClientService.get(cluster)
        .flatMap(client -> client.upsertUserScramCredentials(
            username,
            mechanisms.stream().map(this::mechanism).toList(),
            password
        ));
  }

  @Override
  public Mono<Void> deleteCredential(KafkaCluster cluster, String username, String mechanism) {
    return adminClientService.get(cluster)
        .flatMap(client -> client.deleteUserScramCredential(username, mechanism(mechanism)));
  }

  @Override
  public Mono<Void> deleteUserCredentials(KafkaCluster cluster, String username) {
    return adminClientService.get(cluster)
        .flatMap(client -> client.describeUserScramCredentials()
            .flatMap(description -> {
              var user = description.get(username);
              if (user == null || user.credentialInfos().isEmpty()) {
                return Mono.error(new io.kafbat.ui.exception.NotFoundException(
                    "Kafka SCRAM user not found: " + username));
              }
              return client.alterUserScramCredentials(user.credentialInfos().stream()
                  .map(info -> (org.apache.kafka.clients.admin.UserScramCredentialAlteration)
                      new org.apache.kafka.clients.admin.UserScramCredentialDeletion(username, info.mechanism()))
                  .toList());
            }));
  }

  private ScramMechanism mechanism(String value) {
    return ScramMechanisms.parse(value);
  }
}
